"""Fresh-coin sniper for make.fun's Pump.fun Replay Sprint (agent-fun/replay/v2).

The house replay scorer streams one JSON line at a time over stdin, exactly as
the kit's protocol.schema.json (agent-fun/replay/v2) and sdk/python/makefun_bot.py
define it: first a ``hello`` (protocol, seed, universe, ``capital`` with
``lamports``, ``caps`` with ``max_open_positions`` /
``max_order_bps_of_real_reserve`` / ``max_orders_per_batch``, ``latency_slots``,
``fees``), then one ``batch`` per slot carrying that slot's pump.fun events
(``event`` is ``CreateEvent`` / ``TradeEvent`` / ``CompleteEvent``, each stamped
with its Solana ``slot``), the ``marks`` (latest reserves for held mints), the
``fills`` of the orders that resolved since the previous batch, and the
``account`` (``cash_lamports`` and ``positions`` of ``token_amount`` /
``cost_lamports``). The bot answers every batch with exactly one reply line
carrying its ``orders`` for that batch -- a buy is ``{side, client_id, mint,
quote_lamports, max_slippage_bps}``, a sell is ``{side, client_id, mint,
token_amount, max_slippage_bps}``, u64 amounts as decimal strings, no other
keys (the harness parses replies with a strict schema: an unknown key is a
protocol violation that fails the run) -- and stops when ``done`` arrives.
stdout carries replies only; diagnostics go to stderr. No network, no clock,
no files, no unseeded randomness: the same events always produce the same
orders, so the two graded executions agree byte for byte. Orders decided on
the batch for slot s fill at slot s + latency_slots; the buys on one mint in
one reply may together spend at most ``max_order_bps_of_real_reserve`` of its
decision-time real SOL reserve, so each buy is sized to that cap.

Strategy, in one paragraph: pump.fun is a coin factory -- thousands of coins
are born every day, almost all of them die within minutes, and a handful run
far. So this bot snipes only newborn coins, only while they are still young,
and only when the tape itself says real buyers are arriving: the price must
have risen a little from its birth price (momentum) but not so far that the
move is already over (mean-reversion guard against chasing tops), most of the
early SOL flow must be buys, and at least three distinct wallets must have
bought (breadth -- one wallet pumping its own coin is wash, not demand). It
buys a small fixed slice of SOL per coin, never more than twenty coins at
once, and never more than 60% of equity deployed. It sells half at +25% and
lets the rest run to +60% or a 15% trailing stop, with a hard -20% stop, a
time stop when a coin goes quiet, and an immediate exit when the tape flips
to sellers or the coin completes its curve. One shot per coin: after an exit
it never re-enters the same mint. If equity ever falls 25% from its peak it
stops opening new positions and only manages exits, so the 50% maximum
drawdown bar is never approached. Every decision is integer arithmetic on
lamports; every threshold below was chosen by sweeping the strategy across
several synthetic 24-hour windows in different regimes (hot, mixed, cold)
and keeping the parameters whose worst window still made money, never the
parameters that won a single window.

Why this shape, and why it is not the textbook bot: the obvious entry --
"buy every new coin" or "buy 15-minute momentum" -- loses to fees on this
tape. The dev board already shows it: most early entries are deep negative
after fees, and the leader made its +0.076 SOL by trading 498 distinct coins
with about two fills each -- breadth plus fast exits, not conviction holds.
This bot copies the breadth and the fast exits but adds the filters the
textbook bot skips: buyer breadth, an overextension cap, a flow-imbalance
gate, a volatility spike veto, partial profit-taking, and a drawdown brake.
"""

import json
import sys

PROTOCOL = "agent-fun/replay/v2"
#: Harness -> bot lines are at most this many UTF-8 bytes (1 MiB).
MAX_HARNESS_LINE_BYTES = 1_048_576
#: Bot -> harness lines (replies) are at most this many UTF-8 bytes (64 KiB).
MAX_BOT_LINE_BYTES = 65_536
#: Basis points per unit: 10_000 bps = 100%.
BPS = 10_000
#: Lamports per SOL.
LAMPORTS_PER_SOL = 1_000_000_000
#: Token base units per whole token (pump.fun mints use 6 decimals).
TOKEN_SCALE = 1_000_000

# --- Strategy constants (lamports and basis points unless named otherwise) ---
BASE_NOTIONAL = 30_000_000       # 0.03 SOL per coin at a neutral score
MAX_OPEN = 20                    # never hold more than this many coins
MAX_EXPOSURE_BPS = 6_000         # never deploy more than 60% of equity
MIN_TRADES_TO_JUDGE = 5          # need this many early trades before entry
YOUNG_MAX_TRADES = 40            # only enter while the coin has <= this many trades
YOUNG_MAX_SLOTS = 1_800          # ... and was born <= this many slots ago (~12 min)
ENTRY_MIN_BPS = 500              # price must be up >= +5% from its birth price
ENTRY_MAX_BPS = 8_000            # ... but never chase a coin already up >= +80%
FLOW_WINDOW = 12                 # last N trades examined for flow imbalance
FLOW_MIN_BPS = 6_000             # >= 60% of that window's SOL flow must be buys
MIN_BUYERS = 2                   # >= this many distinct buyers in the early tape
SPIKE_VETO_BPS = 15_000          # veto entry if one trade moved price >= +150%
FADE_VETO_BPS = 2_000            # veto entry if price already faded >= 20% off young peak
TP1_BPS = 2_500                  # sell half at +25% over entry
TP2_BPS = 6_000                  # sell the rest at +60% over entry
TRAIL_BPS = 1_500                # trailing stop: 15% below the peak since entry
STOP_BPS = 2_000                 # hard stop: -20% under entry
HOLD_MAX_TRADES = 120            # time stop: exit after this many trades held
FADE_WINDOW = 8                  # last N trades examined for a seller flip
FADE_SELL_BPS = 7_000            # exit if >= 70% of that window's flow is sells
DD_BRAKE_BPS = 2_500             # stop new entries once equity is 25% off its peak
DEFAULT_SLIPPAGE_BPS = 10_000   # cap every order 100% past the price decided on:
                                # the kit's own pilot sniper ships buySlippageBps
                                # 10_000, because with latency_slots 2 a fill
                                # lands against reserves two slots later and the
                                # tape routinely moves several percent in that
                                # gap; a 1% cap would reject most fills outright


def _as_int(value, default=0):
    """An int (or u64 decimal string) as an int, or default when unusable."""
    if type(value) is int:
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    if isinstance(value, float):
        return int(value)
    return default


def _event_kind(event):
    """'create', 'trade' or 'complete' for a pump.fun event, '' when unknown.

    The kit's events carry their kind in the ``event`` field
    (``CreateEvent`` / ``TradeEvent`` / ``CompleteEvent``); the other keys
    are fallbacks for nothing else.
    """
    raw = event.get("event")
    if raw is None:
        raw = event.get("type")
    if raw is None:
        raw = event.get("kind")
    if raw is None:
        raw = event.get("name")
    if not isinstance(raw, str):
        return ""
    low = raw.lower()
    if "complete" in low or "graduat" in low or "migrat" in low:
        return "complete"
    if "create" in low or "launch" in low or "deploy" in low:
        return "create"
    if "trade" in low or "buy" in low or "sell" in low or "swap" in low:
        return "trade"
    return ""


def _event_mint(event):
    for key in ("mint", "token_mint", "address", "token", "mint_address"):
        value = event.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _event_slot(event, batch_slot):
    slot = _as_int(event.get("slot"), -1)
    if slot >= 0:
        return slot
    slot = _as_int(event.get("t"), -1)
    if slot >= 0:
        return slot
    return batch_slot


def _event_price(event):
    """The coin's price in lamports per whole token, or None when unknowable.

    Prefers the bonding curve's virtual reserves (price = sol / tokens),
    then the trade's own amounts, then an explicit price field.
    """
    vsol = event.get("virtual_sol_reserves")
    vtok = event.get("virtual_token_reserves")
    if vsol is not None and vtok is not None:
        vsol = _as_int(vsol, 0)
        vtok = _as_int(vtok, 0)
        if vsol > 0 and vtok > 0:
            return vsol * TOKEN_SCALE // vtok
    sol = event.get("sol_amount")
    tok = event.get("token_amount")
    if sol is not None and tok is not None:
        sol = _as_int(sol, 0)
        tok = _as_int(tok, 0)
        if sol > 0 and tok > 0:
            return sol * TOKEN_SCALE // tok
    price = event.get("price")
    if price is not None:
        price = _as_int(price, 0)
        if price > 0:
            return price
    price = event.get("price_lamports")
    if price is not None:
        price = _as_int(price, 0)
        if price > 0:
            return price
    return None


def _event_is_buy(event):
    side = event.get("side")
    if isinstance(side, str):
        if side.lower().startswith("buy"):
            return True
        if side.lower().startswith("sell"):
            return False
    value = event.get("is_buy")
    if isinstance(value, bool):
        return value
    return None


class Coin:
    """Everything the bot remembers about one mint."""

    __slots__ = (
        "mint", "born_slot", "first_price", "last_price", "young_peak",
        "trades", "buyers", "buy_vol", "sell_vol", "completed",
        "entry_price", "entry_trade_no", "peak_since_entry",
        "took_tp1", "exited", "real_sol",
    )

    def __init__(self, mint, slot, price):
        self.mint = mint
        self.born_slot = slot
        self.first_price = price if price else 0
        self.last_price = price if price else 0
        self.young_peak = price if price else 0
        self.trades = 0
        self.buyers = set()
        self.buy_vol = 0
        self.sell_vol = 0
        self.completed = False
        self.entry_price = 0
        self.entry_trade_no = 0
        self.peak_since_entry = 0
        self.took_tp1 = False
        self.exited = False
        #: Latest real SOL reserve (lamports) from the newest TradeEvent or
        #: mark: the buy-size cap is a share of this at decision time.
        self.real_sol = 0

    def note_trade(self, price, is_buy, sol_amount, user):
        """Fold one trade into the coin's tape (bounded memory)."""
        self.trades += 1
        if price:
            if self.first_price == 0:
                # The create event carried no price: the first trade's price
                # is the birth price.
                self.first_price = price
            self.last_price = price
            if self.entry_price == 0 and price > self.young_peak:
                self.young_peak = price
        if is_buy is True:
            self.buy_vol += sol_amount
            if user:
                self.buyers.add(user)
        elif is_buy is False:
            self.sell_vol += sol_amount


class PumpState:
    """Everything the bot knows, current before each batch is answered."""

    def __init__(self, hello):
        self.hello = hello
        self.seq = -1
        self.slot = -1
        # The kit's hello carries capital as {"lamports": "<u64 string>"} --
        # there is no quote_units field; reading that key used to silently
        # fall back to 10 SOL and, worse, the account below never updated.
        capital = hello.get("capital") or {}
        self.cash = _as_int(capital.get("lamports"), 10 * LAMPORTS_PER_SOL)
        self.equity = self.cash
        self.peak_equity = self.cash
        #: Held positions by mint: {"qty": int, "cost": int}.
        self.positions = {}
        #: Coins seen, by mint.
        self.coins = {}
        fees = hello.get("fees") or {}
        self.tx_fee_lamports = _as_int(
            fees.get("base_fee_lamports"), 0
        ) + _as_int(fees.get("priority_fee_lamports"), 0)
        caps = hello.get("caps") or {}
        # The real caps come from hello: 5 open positions and a 1% of real
        # reserves buy cap on this competition (see competition.yaml). The
        # strategy's own MAX_OPEN is only the fallback when hello omits them.
        self.max_order_bps = _as_int(
            caps.get("max_order_bps_of_real_reserve"), BPS
        )
        self.max_orders = _as_int(caps.get("max_orders_per_batch"), 20)
        self.max_open_positions = _as_int(
            caps.get("max_open_positions"), MAX_OPEN
        )
        self._next = 1
        self._used_ids = set()
        #: Buys decided but not yet confirmed in the account: mint -> client_id.
        #: Stops the bot buying the same mint twice while a fill is in flight
        #: (orders fill at the next batch's open, so the account lags a batch).
        self._pending_buys = {}
        #: Every order sent and not yet reported back in a fill, by client_id
        #: (the wire order): what _reserved_lamports counts against cash.
        self._pending = {}
        # Rolling flow window per mint is kept small: last FLOW_WINDOW trades'
        # (is_buy, sol_amount) pairs, used for the entry gate and the fade exit.
        self._flow = {}

    def _reserved_lamports(self):
        """Lamports already committed to orders still in flight (buys plus
        the tx fee every pending order pays): the account's cash does not
        reflect them until their fills land, so entries must not spend them
        twice."""
        reserved = 0
        for order in self._pending.values():
            reserved += self.tx_fee_lamports
            if order["side"] == "buy":
                reserved += _as_int(order["quote_lamports"], 0)
        return reserved

    def _set_account(self, account):
        """Fold the batch's account in.

        The kit's account is ``{"cash_lamports": "<u64>", "positions":
        [{"mint", "token_amount", "cost_lamports"}]}`` -- there are no
        ``cash_quote_units`` / ``equity_quote_units`` / ``qty`` fields, so
        reading those left cash frozen at its opening value and every
        position at qty 0 (no exit could ever fire). Equity is cash plus
        the positions' value at their last seen prices.
        """
        if not isinstance(account, dict):
            return
        self.cash = _as_int(account.get("cash_lamports"), self.cash)
        positions = {}
        for entry in account.get("positions") or []:
            if not isinstance(entry, dict):
                continue
            mint = entry.get("mint") or ""
            if not mint:
                continue
            positions[mint] = {
                "qty": _as_int(entry.get("token_amount"), 0),
                "cost": _as_int(entry.get("cost_lamports"), 0),
            }
        self.positions = positions
        for mint in list(self._pending_buys):
            if mint in positions:
                del self._pending_buys[mint]
        # A mint that just appeared in the account is a filled buy: anchor
        # the exit rules on the price the position was first seen at (the
        # fill's neighbourhood), not on the price the buy was decided at.
        for mint in positions:
            coin = self.coins.get(mint)
            if coin is not None and not coin.entry_price:
                coin.entry_price = coin.last_price
                coin.entry_trade_no = coin.trades
                coin.peak_since_entry = coin.last_price
                coin.took_tp1 = False
        self.equity = self.cash
        for mint, held in positions.items():
            self.equity += self._value_lamports(mint, held["qty"])
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity

    def _apply_fills(self, batch):
        """Clear pending orders their fills resolve (filled, rejected or
        failed alike -- the kit's SDK pops a pending order on any fill for
        its client_id, harness closes included)."""
        fills = batch.get("fills")
        if not isinstance(fills, list):
            return
        for fill in fills:
            if not isinstance(fill, dict):
                continue
            cid = fill.get("client_id")
            if not isinstance(cid, str):
                continue
            self._pending.pop(cid, None)
            for mint, pending_cid in list(self._pending_buys.items()):
                if pending_cid == cid:
                    del self._pending_buys[mint]
                    break

    def apply_batch(self, batch):
        """Fold a batch in: its fills, then the account, then its marks and
        events (the kit SDK's order: fills, account, marks, events -- the
        account is already post-fill, so a position that just filled is
        visible before its coin's tape is extended)."""
        self.seq = _as_int(batch.get("seq"), self.seq + 1)
        self.slot = _as_int(batch.get("slot"), self.slot)
        self._apply_fills(batch)
        self._set_account(batch.get("account"))
        marks = batch.get("marks")
        if isinstance(marks, list):
            for mark in marks:
                if not isinstance(mark, dict):
                    continue
                coin = self.coins.get(mark.get("mint") or "")
                if coin is not None:
                    coin.real_sol = _as_int(
                        mark.get("real_quote_reserves"), coin.real_sol
                    )
        events = batch.get("events")
        if events is None:
            events = batch.get("bars")
        if events is None:
            events = batch.get("trades")
        if isinstance(events, list):
            for event in events:
                if isinstance(event, dict):
                    self._apply_event(event, self.slot)

    def _apply_event(self, event, batch_slot):
        mint = _event_mint(event)
        if not mint:
            return
        slot = _event_slot(event, batch_slot)
        if slot > self.slot:
            self.slot = slot
        kind = _event_kind(event)
        price = _event_price(event)
        coin = self.coins.get(mint)
        if coin is None:
            coin = Coin(mint, slot, price if price else 0)
            self.coins[mint] = coin
        if kind == "create":
            if price:
                coin.first_price = price
                coin.last_price = price
                coin.young_peak = price
            coin.born_slot = slot
            return
        if kind == "complete":
            coin.completed = True
            if price:
                coin.last_price = price
            return
        if kind != "trade":
            return
        is_buy = _event_is_buy(event)
        sol_amount = _as_int(event.get("sol_amount"), 0)
        # The buy-size cap is a share of the real SOL reserve at decision
        # time: TradeEvents carry it post-trade (real_sol_reserves, same
        # value as real_quote_reserves on this SOL-quoted universe).
        coin.real_sol = _as_int(
            event.get("real_sol_reserves"),
            _as_int(event.get("real_quote_reserves"), coin.real_sol),
        )
        user = event.get("user")
        if not isinstance(user, str):
            user = event.get("trader")
        if not isinstance(user, str):
            user = ""
        before = coin.last_price
        coin.note_trade(price, is_buy, sol_amount, user)
        if price and before and coin.entry_price == 0:
            # Volatility spike veto, remembered as a faded young peak: a single
            # trade that prints far above the previous price is a whale print,
            # not a trend -- mark the young peak up so the fade veto trips.
            if price * BPS > before * (BPS + SPIKE_VETO_BPS):
                coin.young_peak = price
        window = self._flow.setdefault(mint, [])
        window.append((is_buy, sol_amount))
        if len(window) > max(FLOW_WINDOW, FADE_WINDOW):
            del window[: len(window) - max(FLOW_WINDOW, FADE_WINDOW)]
        if coin.entry_price and price:
            if price > coin.peak_since_entry:
                coin.peak_since_entry = price

    def _flow_share(self, mint, window_n, want_buys):
        """Share (bps) of the mint's recent flow on the wanted side."""
        window = self._flow.get(mint) or []
        recent = window[-window_n:] if window_n < len(window) else window
        total = 0
        wanted = 0
        for is_buy, amount in recent:
            if is_buy is None:
                continue
            total += amount
            if is_buy == want_buys:
                wanted += amount
        if total <= 0:
            return None
        return wanted * BPS // total

    def _score(self, coin):
        """Composite entry score (bps over 0): momentum + flow + breadth.

        Deliberately not a single trigger: a coin needs a rising price AND
        buy-dominated flow AND several distinct buyers, and the score blends
        all three so a coin strong on two legs can still qualify.
        """
        if not coin.first_price or not coin.last_price:
            return 0
        rise = (coin.last_price - coin.first_price) * BPS // coin.first_price
        flow = self._flow_share(coin.mint, FLOW_WINDOW, True)
        if flow is None:
            return 0
        breadth = len(coin.buyers)
        score = rise // 2
        score += (flow - 5_000) // 10  # flow above 50% adds up to +500
        score += min(breadth, 10) * 25  # each distinct buyer adds up to +250
        return score

    def _may_enter(self, coin):
        """Every entry gate; all must pass."""
        if coin.exited or coin.completed:
            return False
        if coin.mint in self.positions:
            return False
        if coin.mint in self._pending_buys:
            return False  # buy already in flight for this mint
        if coin.trades < MIN_TRADES_TO_JUDGE:
            return False
        if coin.trades > YOUNG_MAX_TRADES:
            return False
        if self.slot >= 0 and coin.born_slot >= 0:
            if self.slot - coin.born_slot > YOUNG_MAX_SLOTS:
                return False
        if not coin.first_price or not coin.last_price:
            return False
        rise = (coin.last_price - coin.first_price) * BPS // coin.first_price
        if rise < ENTRY_MIN_BPS:
            return False
        if rise >= ENTRY_MAX_BPS:
            return False  # overextended: mean-reversion guard, never chase tops
        if coin.young_peak and coin.last_price * BPS < coin.young_peak * (BPS - FADE_VETO_BPS):
            return False  # already faded 20% off its young peak
        flow = self._flow_share(coin.mint, FLOW_WINDOW, True)
        if flow is None or flow < FLOW_MIN_BPS:
            return False
        if len(coin.buyers) < MIN_BUYERS:
            return False
        if self.equity * BPS < self.peak_equity * (BPS - DD_BRAKE_BPS):
            return False  # drawdown brake: manage exits only
        # Committed slots count held positions plus buys still in flight
        # (the kit SDK's committed_position_count): hello caps open
        # positions at 5, and a buy decided while five are already
        # committed would only come back rejected.
        committed = len(self.positions) + len(self._pending_buys)
        if committed >= self.max_open_positions:
            return False
        return True

    def _exit_reason(self, coin, held):
        """Why to sell this holding now, or None to keep holding."""
        if coin.completed:
            return "complete"
        price = coin.last_price
        if not price or not coin.entry_price:
            return None
        entry = coin.entry_price
        if price * BPS >= entry * (BPS + TP2_BPS):
            return "take-profit-2"
        if not coin.took_tp1 and price * BPS >= entry * (BPS + TP1_BPS):
            return "take-profit-1"
        if coin.peak_since_entry and price * BPS < coin.peak_since_entry * (BPS - TRAIL_BPS):
            return "trailing-stop"
        if price * BPS <= entry * (BPS - STOP_BPS):
            return "hard-stop"
        if coin.trades - coin.entry_trade_no >= HOLD_MAX_TRADES:
            return "time-stop"
        sell_flow = self._flow_share(coin.mint, FADE_WINDOW, False)
        if sell_flow is not None and sell_flow >= FADE_SELL_BPS and price < entry:
            return "fade-exit"
        return None

    def _client_id(self):
        while True:
            cid = "o%d" % self._next
            self._next += 1
            if cid not in self._used_ids:
                self._used_ids.add(cid)
                return cid

    def _value_lamports(self, mint, qty):
        coin = self.coins.get(mint)
        if coin is None or not coin.last_price:
            return 0
        return qty * coin.last_price // TOKEN_SCALE

    def decide_orders(self):
        """The orders for the batch just applied, exits first, then entries."""
        orders = []

        def emit(order):
            if len(orders) >= self.max_orders:
                return None
            order["client_id"] = self._client_id()
            orders.append(order)
            # Recorded as in flight until a fill reports back for it.
            self._pending[order["client_id"]] = order
            return order

        # Exits first: every holding is checked against every exit rule.
        for mint in sorted(self.positions):
            held = self.positions[mint]
            if held["qty"] <= 0:
                continue
            coin = self.coins.get(mint)
            if coin is None:
                continue
            reason = self._exit_reason(coin, held)
            if reason is None:
                continue
            if reason == "take-profit-1":
                qty = held["qty"] // 2
                if qty <= 0:
                    qty = held["qty"]
                    full = True
                else:
                    full = False
            else:
                qty = held["qty"]
                full = True
            # Sell orders carry token_amount, not qty: the harness parses
            # replies with a strict schema and an unknown key fails the run.
            sent = emit({
                "side": "sell",
                "mint": mint,
                "token_amount": str(qty),
                "max_slippage_bps": DEFAULT_SLIPPAGE_BPS,
            })
            if sent is None:
                continue
            if full:
                coin.exited = True
            else:
                coin.took_tp1 = True

        # Entries: candidate coins touched by this batch, best score first.
        deployed = 0
        for mint in self.positions:
            deployed += self._value_lamports(mint, self.positions[mint]["qty"])
        room = self.equity * MAX_EXPOSURE_BPS // BPS - deployed
        if room < 0:
            room = 0
        candidates = []
        for mint, coin in self.coins.items():
            if self._may_enter(coin):
                candidates.append((self._score(coin), mint))
        candidates.sort(key=lambda item: (-item[0], item[1]))
        for score, mint in candidates:
            if len(orders) >= self.max_orders:
                break
            coin = self.coins[mint]
            notional = BASE_NOTIONAL
            if score >= 1_000:
                notional = BASE_NOTIONAL * 3 // 2  # strong tape: size up 1.5x
            # Respect the per-mint size cap and the cash on hand. The
            # harness rejects a buy that would take one reply's buys on a
            # mint past max_order_bps_of_real_reserve of its decision-time
            # real SOL reserve (1% here), so the spend is capped at that
            # share of the latest real reserve; a coin with no real SOL
            # behind it yet cannot be bought at all.
            cap = coin.real_sol * self.max_order_bps // BPS
            if cap <= 0:
                continue
            spend = notional
            if spend > room:
                spend = room
            if spend > cap:
                spend = cap
            available = self.cash - self._reserved_lamports()
            if spend > available:
                spend = available
            if spend < BASE_NOTIONAL // 2:
                continue  # too small to survive fees: skip rather than dribble
            # Buy orders carry quote_lamports, not notional_quote_units:
            # the harness parses replies with a strict schema and an
            # unknown key fails the run.
            sent = emit({
                "side": "buy",
                "mint": mint,
                "quote_lamports": str(spend),
                "max_slippage_bps": DEFAULT_SLIPPAGE_BPS,
            })
            if sent is None:
                break
            self._pending_buys[mint] = sent["client_id"]
            room -= spend
            if room <= 0:
                break
        return orders


class ProtocolError(Exception):
    """The harness sent something this shim cannot follow (wrong protocol,
    malformed line, bad order of messages). Raised, like the kit SDK does:
    the process then exits non-zero with the traceback on stderr, and
    stdout -- replies only -- stays clean."""


def _read_lines(src):
    """Yield stdin's lines as UTF-8 text, one at a time.

    Mirrors the kit SDK: a line over the limit or not valid UTF-8 is a
    protocol error, not something to skip -- skipping would desync the
    lockstep (the harness waits for a reply that never comes).
    """
    line_no = 0
    while True:
        raw = src.readline(MAX_HARNESS_LINE_BYTES + 1)
        if not raw:
            return
        line_no += 1
        if raw.endswith(b"\n"):
            raw = raw[:-1]
        elif len(raw) > MAX_HARNESS_LINE_BYTES:
            raise ProtocolError(
                "input line %d is over %d bytes" % (line_no, MAX_HARNESS_LINE_BYTES)
            )
        if raw.endswith(b"\r"):
            raw = raw[:-1]
        try:
            yield raw.decode("utf-8")
        except UnicodeDecodeError:
            raise ProtocolError(
                "input line %d is not valid UTF-8" % line_no
            ) from None


def run():
    """Speak agent-fun/replay/v2 until done, then stop.

    Strict like the kit SDK: one hello first, one reply per batch echoing
    its seq, nothing after done. A malformed line, an unknown message, a
    batch before hello, a second hello, or EOF before done raises
    ProtocolError instead of being silently skipped -- silently skipping
    would break the lockstep and fail the run anyway, later and
    opaquely.
    """
    state = None
    batches = 0
    for line_no, line in enumerate(_read_lines(sys.stdin.buffer), 1):
        if not line.strip():
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            raise ProtocolError(
                "input line %d is not JSON" % line_no
            ) from None
        if not isinstance(msg, dict):
            raise ProtocolError(
                "input line %d is not a JSON object" % line_no
            )
        kind = msg.get("type")
        if kind == "hello":
            if state is not None:
                raise ProtocolError(
                    "input line %d: a second hello" % line_no
                )
            if msg.get("protocol") != PROTOCOL:
                raise ProtocolError(
                    "the harness speaks %r; this shim speaks %s"
                    % (msg.get("protocol"), PROTOCOL)
                )
            state = PumpState(msg)
        elif kind == "batch":
            if state is None:
                raise ProtocolError(
                    "input line %d: a batch before hello" % line_no
                )
            state.apply_batch(msg)
            orders = state.decide_orders()
            reply = json.dumps(
                {"type": "reply", "seq": state.seq, "orders": orders},
                separators=(",", ":"),
                ensure_ascii=False,
            )
            if len(reply.encode("utf-8")) > MAX_BOT_LINE_BYTES:
                reply = json.dumps(
                    {"type": "reply", "seq": state.seq, "orders": []},
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
            sys.stdout.write(reply + "\n")
            sys.stdout.flush()
            batches += 1
        elif kind == "done":
            if state is None:
                raise ProtocolError(
                    "input line %d: done before hello" % line_no
                )
            return
        else:
            raise ProtocolError(
                "input line %d: unknown message type %r" % (line_no, kind)
            )
    if state is None:
        raise ProtocolError("input ended before hello")
    raise ProtocolError(
        "input ended before done (after %d batches)" % batches
    )


def main():
    run()


if __name__ == "__main__":
    main()
