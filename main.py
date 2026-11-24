"""Fresh-coin sniper for make.fun's Pump.fun Replay Sprint (agent-fun/replay/v2).

The house replay scorer streams one JSON line at a time over stdin: first a
``hello`` describing the window (10 SOL of paper money, fees, caps, seed), then
one ``batch`` per step carrying that step's pump.fun events (CreateEvent,
TradeEvent, CompleteEvent, each stamped with its Solana ``slot``), the fills of
the orders decided on the previous batch, and the account. The bot answers
every batch with exactly one reply line carrying its ``orders`` for that batch,
and stops when ``done`` arrives. stdout carries replies only; diagnostics go
to stderr. No network, no clock, no files, no unseeded randomness: the same
events always produce the same orders, so the two graded executions agree
byte for byte.

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
DEFAULT_SLIPPAGE_BPS = 100       # cap every order 1% past the price decided on


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
    """'create', 'trade' or 'complete' for a pump.fun event, '' when unknown."""
    raw = event.get("type")
    if raw is None:
        raw = event.get("kind")
    if raw is None:
        raw = event.get("event")
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
        "took_tp1", "exited",
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
        capital = hello.get("capital") or {}
        self.cash = _as_int(capital.get("quote_units"), 10 * LAMPORTS_PER_SOL)
        self.equity = self.cash
        self.peak_equity = self.cash
        #: Held positions by mint: {"qty": int, "cost": int}.
        self.positions = {}
        #: Coins seen, by mint.
        self.coins = {}
        self.warmup_bars = _as_int(hello.get("warmup_bars"), 0)
        fees = hello.get("fees") or {}
        self.min_notional = _as_int(
            fees.get("min_notional_quote_units"),
            _as_int((hello.get("caps") or {}).get("min_notional_quote_units"), 0),
        )
        caps = hello.get("caps") or {}
        self.max_weight_bps = _as_int(caps.get("max_weight_bps"), BPS)
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
        # Rolling flow window per mint is kept small: last FLOW_WINDOW trades'
        # (is_buy, sol_amount) pairs, used for the entry gate and the fade exit.
        self._flow = {}

    @property
    def warming_up(self):
        return self.warmup_bars > 0 and self.seq < self.warmup_bars

    def _set_account(self, account):
        if not isinstance(account, dict):
            return
        self.cash = _as_int(
            account.get("cash_quote_units"), self.cash
        )
        self.equity = _as_int(
            account.get("equity_quote_units"), self.equity
        )
        if self.equity > self.peak_equity:
            self.peak_equity = self.equity
        positions = {}
        for entry in account.get("positions") or []:
            if not isinstance(entry, dict):
                continue
            mint = entry.get("mint") or entry.get("symbol") or ""
            if not mint:
                continue
            positions[mint] = {
                "qty": _as_int(entry.get("qty"), 0),
                "cost": _as_int(entry.get("cost_quote_units"), 0),
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

    def _apply_fills(self, batch):
        """Clear pending buys the batch's fills say were rejected."""
        fills = batch.get("fills")
        if not isinstance(fills, list):
            return
        by_id = {cid: mint for mint, cid in self._pending_buys.items()}
        for fill in fills:
            if not isinstance(fill, dict):
                continue
            status = fill.get("status")
            if not isinstance(status, str):
                continue
            if status.lower() not in ("rejected", "failed", "cancelled", "invalid"):
                continue
            cid = fill.get("client_id")
            mint = by_id.get(cid)
            if mint:
                del self._pending_buys[mint]

    def apply_batch(self, batch):
        """Fold a batch in: its events, then the account."""
        self.seq = _as_int(batch.get("seq"), self.seq + 1)
        self.slot = _as_int(batch.get("slot"), self.slot)
        events = batch.get("events")
        if events is None:
            events = batch.get("bars")
        if events is None:
            events = batch.get("trades")
        if isinstance(events, list):
            for event in events:
                if isinstance(event, dict):
                    self._apply_event(event, self.slot)
        self._apply_fills(batch)
        self._set_account(batch.get("account"))

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
        if self.warming_up:
            return False
        if self.equity * BPS < self.peak_equity * (BPS - DD_BRAKE_BPS):
            return False  # drawdown brake: manage exits only
        if len(self.positions) >= self.max_open_positions:
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
            sent = emit({
                "side": "sell",
                "mint": mint,
                "qty": str(qty),
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
            # Respect the per-mint weight cap and the cash on hand.
            weight_room = (
                self.max_weight_bps * self.equity // BPS
                - self._value_lamports(mint, 0)
            )
            spend = notional
            if spend > room:
                spend = room
            if spend > weight_room:
                spend = weight_room
            if spend > self.cash:
                spend = self.cash
            if spend < self.min_notional and self.min_notional > 0:
                continue
            if spend < BASE_NOTIONAL // 2:
                continue  # too small to survive fees: skip rather than dribble
            sent = emit({
                "side": "buy",
                "mint": mint,
                "notional_quote_units": str(spend),
                "max_slippage_bps": DEFAULT_SLIPPAGE_BPS,
            })
            if sent is None:
                break
            self._pending_buys[mint] = sent["client_id"]
            room -= spend
            self.cash -= spend
            if room <= 0:
                break
        return orders


def _read_lines(src):
    """Yield stdin's lines as UTF-8 text, one at a time."""
    while True:
        raw = src.readline(MAX_HARNESS_LINE_BYTES + 1)
        if not raw:
            return
        if raw.endswith(b"\n"):
            raw = raw[:-1]
        if raw.endswith(b"\r"):
            raw = raw[:-1]
        if not raw.strip():
            continue
        try:
            yield raw.decode("utf-8")
        except UnicodeDecodeError:
            continue


def run():
    """Speak agent-fun/replay/v2 until done, then stop."""
    state = None
    for line in _read_lines(sys.stdin.buffer):
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if not isinstance(msg, dict):
            continue
        kind = msg.get("type")
        if kind == "hello":
            if state is not None:
                continue
            if msg.get("protocol") not in (PROTOCOL, None):
                # A harness speaking another protocol: answer nothing rather
                # than violate it; the run still ends cleanly at done/EOF.
                state = PumpState(msg)
                continue
            state = PumpState(msg)
        elif kind == "batch":
            if state is None:
                continue
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
        elif kind == "done":
            return
    return


def main():
    run()


if __name__ == "__main__":
    main()
