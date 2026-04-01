# Polymarket Orderbook Collector

Streams live order book data from Polymarket for 5 crypto assets across 5 time intervals.
Stores one CSV row per second per market, plus a resolved outcome row when each market closes.

Everything is self-contained in this folder — venv, logs, and data all live here.

---

## Quick start

```bash
cd pmcts
./collector.sh start
```

That's it. On the first run it automatically:
1. Finds Python 3.9+ on your system
2. Creates a `.venv` inside this folder
3. Installs `requests` and `websockets`
4. Launches the collector in a background tmux session

---

## Commands

| Command | What it does |
|---|---|
| `./collector.sh start` | Auto-setup if needed, then launch in tmux |
| `./collector.sh stop` | Kill the running session |
| `./collector.sh restart` | Stop + start |
| `./collector.sh attach` | Re-attach to the tmux session |
| `./collector.sh status` | Show tick/outcome counts per market |
| `./collector.sh logs` | Tail live log output |
| `./collector.sh setup` | Create venv + install deps (explicit) |

---

## What it collects

**Assets:** BTC, ETH, SOL, XRP, BNB

**Intervals:** 5m, 15m, 1h, 4h, 1d

**25 markets running in parallel** (one thread per asset × interval pair).

### Tick data — `data/{asset}/{interval}/orderbook_ticks.csv`

One row written per second. Each row is a full order book snapshot at that moment:

| Field | Description |
|---|---|
| `timestamp_utc` | Wall-clock time (UTC) |
| `unix_ts` | Unix timestamp (integer seconds) |
| `seconds_remaining` | Seconds until this market closes |
| `up_best_bid/ask` | Best bid and ask for the UP token |
| `dn_best_bid/ask` | Best bid and ask for the DOWN token |
| `up_midpoint` / `dn_midpoint` | (bid + ask) / 2 — implied probability |
| `price_sum` | up_mid + dn_mid (should be ≈ 1.0 in efficient markets) |
| `up_imbalance` | up_total_bid / (up_total_bid + up_total_ask) |
| `up_bid_price_0..9` / `up_ask_price_0..9` | Top 10 levels of the UP order book |
| `dn_bid_price_0..9` / `dn_ask_price_0..9` | Top 10 levels of the DOWN order book |

### Outcome data — `data/{asset}/{interval}/market_outcomes.csv`

One row written when each market window closes.

| Field | Description |
|---|---|
| `outcome` | `UP` or `DOWN` |
| `up_open` / `up_close` | UP token midpoint at open and close |
| `dn_open` / `dn_close` | DOWN token midpoint at open and close |
| `total_ticks` | Number of 1-second rows collected |
| `resolved_at` | When the outcome was written |

**Outcome resolution logic (in priority order):**
1. **Last-second tick** — when `seconds_remaining == 0`, if either side's best bid hits ≥ 0.99, that side won. This is the most reliable signal.
2. **Polymarket REST** — after a 5-second settlement delay, checks `outcomePrices` from the Gamma API.
3. **Closing midpoint** — whichever side's probability was above 0.5 at close.

---

## Data layout

```
collector/
├── collect.py          # collector logic
├── collector.sh        # launcher (start/stop/status/logs)
├── .venv/              # created on first run, not tracked in git
├── collector.log       # live log output, not tracked in git
└── data/               # all collected CSVs, not tracked in git
    ├── btc/
    │   ├── 5m/
    │   │   ├── orderbook_ticks.csv
    │   │   └── market_outcomes.csv
    │   ├── 15m/
    │   ├── 1h/
    │   ├── 4h/
    │   └── 1d/
    ├── eth/
    ├── sol/
    ├── xrp/
    └── bnb/
```

---

## How it works

### Market discovery

Polymarket uses time-windowed binary markets. Each market resolves at the end of its interval
(e.g. "Will BTC go up in the next 5 minutes?"). The collector discovers the current market slug
via the Gamma REST API, then subscribes to its order book via WebSocket.

Slug formats differ by interval:
- **5m / 15m / 4h** — `btc-updown-5m-{unix_timestamp}`
- **1h** — `bitcoin-up-or-down-april-1-2026-6am-et` (Eastern time)
- **1d** — `bitcoin-up-or-down-on-april-1-2026` (UTC date)

### Data flow

```
Polymarket WebSocket (wss://ws-subscriptions-clob.polymarket.com/ws/market)
  └─► LiveBook.apply_snapshot() / apply_change()   ← every WS message
        └─► on_tick()                               ← throttled to 1/sec
              └─► _append(orderbook_ticks.csv)      ← one row written per second
```

On market close:
```
Window ends → WSFeed.stop()
  └─► Outcome resolution (3-tier: last-second bid → REST → midpoint)
        └─► _append(market_outcomes.csv)   ← one row per closed market
              └─► Discover next window → reconnect WS
```

### REST cache

A background thread refreshes spread and last-trade price from the CLOB REST API every 30
seconds per market. These are merged into each tick row but don't affect the 1-second cadence.

---

## Requirements

- **Python 3.9+** (for `zoneinfo`; auto-detected by `collector.sh`)
- **tmux** (for background session management)
- No API keys required — uses Polymarket's public WebSocket

### Manual setup (if you prefer not to use the shell script)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install "requests>=2.31.0" "websockets>=12.0"

# Run all markets
python collect.py

# Run a subset
python collect.py --assets btc eth --intervals 5m 15m
```
