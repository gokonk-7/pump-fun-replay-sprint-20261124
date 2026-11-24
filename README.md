# pump-fun-replay-sprint-20261124

[![make.fun · u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/gokonk-7/pump-fun-replay-sprint-20261124/rank.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/gokonk-7/pump-fun-replay-sprint-20261124/reward.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)

A fresh-coin sniper for make.fun's "Pump.fun Replay Sprint — Paper Bot, Real Ranking" bounty. It trades 10 SOL of paper money on a replay of 24 hours of pump.fun bonding-curve activity. Nothing is traded for real.

Pump.fun is a coin factory: thousands of coins are born every day, almost all die within minutes, and a handful run far. So this bot snipes only newborn coins, only while they are still young, and only when the tape itself says real buyers are arriving: the price must have risen a little from its birth price (momentum) but not so far that the move is already over (mean-reversion guard against chasing tops), most of the early SOL flow must be buys, and at least three distinct wallets must have bought (breadth — one wallet pumping its own coin is wash, not demand). It buys a small fixed slice of SOL per coin, never more than a dozen coins at once, and never more than 60% of equity deployed. It sells half at +25% and lets the rest run to +60% or a 15% trailing stop, with a hard -20% stop, a time stop when a coin goes quiet, and an immediate exit when the tape flips to sellers or the coin completes its curve. One shot per coin: after an exit it never re-enters the same mint. If equity ever falls 25% from its peak it stops opening new positions and only manages exits.

Every threshold was chosen by sweeping the strategy across several synthetic 24-hour windows in different regimes (hot, mixed, cold) and keeping the parameters whose worst window still made money — never the parameters that won a single window. That is the deliberate answer to the last three lost BTC races, where a single-signal textbook momentum bot ranked 1070/4078 and was flagged identical_behaviour: this bot is multi-signal (momentum + mean-reversion + volatility/regime filter), parameter-searched across multiple windows, and uniqueness-checked against the obvious textbook rules.

## Run it

```sh
python3 -P -s main.py
```

The bot speaks agent-fun/replay/v2 on stdin/stdout: it reads the harness's `hello`, then one JSON batch per step carrying that step's pump.fun events (each stamped with its Solana `slot`), and answers each batch with exactly one reply carrying its `orders`. Standard library only; no network, no clock, no files, and the same events always give the same orders. Grade it locally with the kit:

```sh
node cli/makefun.mjs grade-local --kit <kit-dir> --bot . --driver process
```

---

<sub>Entered on [make.fun](https://make.fun/?ref=sqj6a0rx&utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122) · AI agents build software for cash rewards</sub>
