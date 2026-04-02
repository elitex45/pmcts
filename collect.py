"""
Polymarket multi-asset, multi-interval orderbook collector.

One thread per (asset, interval) pair — each discovers the current Polymarket
up/down market, streams WS tick data, then rotates to the next interval.

Data layout (parquet, default):
  data/{asset}/{interval}/ticks/{interval_ts}.parquet  — all ticks for one window
  data/{asset}/{interval}/market_outcomes.parquet      — one row per closed market

Data layout (csv, --format csv):
  data/{asset}/{interval}/orderbook_ticks.csv   — tick on every WS book change
  data/{asset}/{interval}/market_outcomes.csv   — one row per closed market

Slug formats:
  5m / 15m / 4h  →  {asset}-updown-{interval}-{ts}
                    e.g. btc-updown-5m-1774466400
  1h             →  {full_name}-up-or-down-{month}-{day}-{year}-{hour}am/pm-et
                    e.g. bitcoin-up-or-down-april-1-2026-6am-et
  1d             →  {full_name}-up-or-down-on-{month}-{day}-{year}
                    e.g. bitcoin-up-or-down-on-april-1-2026

Run: python collect.py [--assets btc eth] [--intervals 5m 15m] [--format csv|parquet]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import statistics
import threading
import time
from collections import deque
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from typing import Any, Callable

import requests
from websockets.sync.client import connect

# ── Markets config ────────────────────────────────────────────────────────────
ALL_ASSETS    = ["btc", "eth", "sol", "xrp", "bnb"]
ALL_INTERVALS = [("5m", 300), ("15m", 900), ("1h", 3600), ("4h", 14400), ("1d", 86400)]

# Full names used in 1h / 1d human-readable slugs
ASSET_FULL = {
    "btc": "bitcoin", "eth": "ethereum", "sol": "solana",
    "xrp": "xrp",     "bnb": "bnb",
}
ET = ZoneInfo("America/New_York")   # handles DST automatically

def _build_slug(asset: str, interval: str, interval_ts: int) -> str:
    """Build the Polymarket event slug for a given asset/interval/timestamp."""
    if interval in ("5m", "15m", "4h"):
        return f"{asset}-updown-{interval}-{interval_ts}"

    full = ASSET_FULL.get(asset, asset)

    if interval == "1h":
        # Slug is keyed by ET (Eastern) hour at the interval start
        dt = datetime.fromtimestamp(interval_ts, tz=ET)
        h = dt.hour
        hstr = ("12am" if h == 0 else f"{h}am" if h < 12
                else "12pm" if h == 12 else f"{h - 12}pm")
        return (f"{full}-up-or-down-"
                f"{dt.strftime('%B').lower()}-{dt.day}-{dt.year}-{hstr}-et")

    if interval == "1d":
        dt = datetime.fromtimestamp(interval_ts, tz=timezone.utc)
        return f"{full}-up-or-down-on-{dt.strftime('%B').lower()}-{dt.day}-{dt.year}"

    return f"{asset}-updown-{interval}-{interval_ts}"   # fallback


# ── Binance config ────────────────────────────────────────────────────────────
BINANCE_REST     = "https://api.binance.com/api/v3"
BINANCE_SYMBOLS  = {
    "btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT",
    "xrp": "XRPUSDT", "bnb": "BNBUSDT",
}
# Polymarket interval → Binance kline interval
BINANCE_INTERVAL = {
    "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d",
}
VOL_INTERVAL     = 60    # sample one price per 60s for vol
VOL_SAMPLES      = 10    # 10 minutes of vol history
FALLBACK_VOL     = 0.80  # annualized vol until 2+ samples collected


CLOB_HOST        = "https://clob.polymarket.com"
GAMMA_HOST       = "https://gamma-api.polymarket.com"
WS_URL           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ORDER_BOOK_DEPTH = 10
REST_REFRESH_SEC = 30
DATA_DIR         = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
FLUSH_INTERVAL   = 60    # write parquet every N seconds (partial flush)

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

_stop_event = threading.Event()   # set on SIGINT/SIGTERM — all threads flush and exit


# ── Fair value (pure Python — no scipy dependency) ────────────────────────────
_SECONDS_PER_YEAR = 31_557_600   # 365.25 * 24 * 3600

def _norm_cdf(x: float) -> float:
    """Normal CDF via Abramowitz & Stegun rational approximation (max error 7.5e-8)."""
    a1, a2, a3, a4, a5 = 0.319381530, -0.356563782, 1.781477937, -1.821255978, 1.330274429
    p = 0.2316419
    k = 1.0 / (1.0 + p * abs(x))
    poly = k * (a1 + k * (a2 + k * (a3 + k * (a4 + k * a5))))
    cdf = 1.0 - (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x) * poly
    return cdf if x >= 0.0 else 1.0 - cdf

def _fair_value(spot: float, window_open: float, secs_remaining: float, vol: float) -> float:
    """Probability that asset finishes above window_open. Mirrors bot/fair_value.py."""
    if window_open <= 0 or spot <= 0:
        return 0.5
    if secs_remaining <= 0:
        return 1.0 if spot >= window_open else 0.0
    if vol <= 0:
        delta = spot - window_open
        return 1.0 if delta > 0 else (0.0 if delta < 0 else 0.5)
    delta = (spot - window_open) / window_open
    sigma = vol * math.sqrt(secs_remaining / _SECONDS_PER_YEAR)
    if sigma <= 0:
        return 1.0 if delta >= 0 else 0.0
    return _norm_cdf(delta / sigma)


# ── CSV schema ────────────────────────────────────────────────────────────────

def _tick_fields(depth: int = ORDER_BOOK_DEPTH) -> list[str]:
    f = ["timestamp_utc", "unix_ts", "market_type", "market_slug", "condition_id",
         "interval_start_ts", "interval_end_ts", "seconds_remaining",
         "up_token_id", "down_token_id",
         "up_best_bid", "up_best_ask", "up_midpoint", "up_price", "up_spread", "up_last_trade",
         "up_total_bid", "up_total_ask", "up_imbalance", "up_bid_lvls", "up_ask_lvls",
         "dn_best_bid", "dn_best_ask", "dn_midpoint", "dn_price", "dn_spread", "dn_last_trade",
         "dn_total_bid", "dn_total_ask", "dn_imbalance", "dn_bid_lvls", "dn_ask_lvls",
         "implied_up", "implied_dn", "price_sum",
         # ── Binance spot fields (for backtesting) ─────────────────────────────
         "btc_price",         # current spot price from Binance (named btc_price for backtest compat)
         "window_open_price", # Binance kline open at interval start
         "btc_vol_ann",       # annualized realized vol (same formula as bot)
         "fair_value",        # P(asset > window_open at expiry) — Black-Scholes CDF
         "poly_yes_price",    # alias for up_midpoint (backtest compat)
         "gap",               # fair_value - poly_yes_price
         ]
    for pfx in ("up", "dn"):
        for i in range(depth):
            f += [f"{pfx}_bid_price_{i}", f"{pfx}_bid_size_{i}",
                  f"{pfx}_ask_price_{i}", f"{pfx}_ask_size_{i}"]
    return f

TICK_FIELDS = _tick_fields()
OUTCOME_FIELDS = [
    "market_type", "market_slug", "condition_id",
    "interval_start_ts", "interval_end_ts", "interval_start_utc", "interval_end_utc",
    "up_token_id", "down_token_id",
    "outcome", "up_open", "up_close", "dn_open", "dn_close",
    "total_ticks", "resolved_at",
]

def _ensure(path: str, fields: list[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()

def _append(path: str, fields: list[str], row: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow(row)


# ── Parquet helpers ───────────────────────────────────────────────────────────

class ParquetWindowWriter:
    """
    Streaming parquet writer for one market window.

    Each flush writes only the new rows as a fresh row group — O(new rows),
    never O(total rows). The file is valid parquet throughout (multi-row-group).
    Call close() when the window ends or on stop signal.
    """

    def __init__(self, path: str):
        import pyarrow as pa
        import pyarrow.parquet as pq
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._pa     = pa
        self._schema = pa.schema([pa.field(f, pa.string()) for f in TICK_FIELDS])
        self._writer = pq.ParquetWriter(path, self._schema, compression="snappy")
        self._flushed = 0   # rows already written

    def flush(self, rows: list[dict]) -> int:
        """Write rows[self._flushed:] as a new row group. Returns count written."""
        new = rows[self._flushed:]
        if not new:
            return 0
        tbl = self._pa.Table.from_pydict(
            {f: [str(r.get(f, "")) for r in new] for f in TICK_FIELDS},
            schema=self._schema,
        )
        self._writer.write_table(tbl)
        self._flushed += len(new)
        return len(new)

    def close(self):
        self._writer.close()

def _append_outcomes_parquet(path: str, row: dict):
    """Append one outcome row to the outcomes parquet file (read-concat-write)."""
    import pandas as pd
    new_df = pd.DataFrame([row], columns=OUTCOME_FIELDS)
    if os.path.exists(path):
        existing = pd.read_parquet(path, engine="pyarrow")
        df = pd.concat([existing, new_df], ignore_index=True)
    else:
        df = new_df
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")


# ── REST helpers ──────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = 6) -> dict:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

def _fetch_spread(tid: str) -> float | None:
    v = _get(f"{CLOB_HOST}/spread?token_id={tid}").get("spread")
    return float(v) if v else None

def _fetch_last(tid: str) -> float | None:
    v = _get(f"{CLOB_HOST}/last-trade-price?token_id={tid}").get("price")
    return float(v) if v else None

def _fetch_price(tid: str) -> float | None:
    v = _get(f"{CLOB_HOST}/midpoint?token_id={tid}").get("mid")
    return float(v) if v else None

def _fetch_event(slug: str) -> dict | None:
    try:
        d = requests.get(f"{GAMMA_HOST}/events?slug={slug}", timeout=10).json()
        return d[0] if isinstance(d, list) and d else None
    except Exception as e:
        log.warning("Gamma error: %s", e)
        return None

def _wait_for_market(slug: str, max_wait: int = 120) -> dict | None:
    deadline = time.time() + max_wait
    while time.time() < deadline:
        ev = _fetch_event(slug)
        if ev:
            return ev
        log.info("Waiting for %s…", slug)
        time.sleep(5)
    return None

def _extract_tokens(event: dict) -> tuple[str, str, str]:
    m    = event["markets"][0]
    ids  = json.loads(m.get("clobTokenIds", "[]"))
    outs = json.loads(m.get("outcomes", "[]"))
    up = dn = None
    for i, o in enumerate(outs):
        if o.upper() in ("UP", "YES"):  up = ids[i]
        elif o.upper() in ("DOWN","NO"): dn = ids[i]
    if not up or not dn:
        up, dn = ids[0], ids[1]
    return m.get("conditionId", ""), up, dn

def _flt(v) -> float | None:
    try:   return float(v) if v is not None and v != "" else None
    except: return None


# ── Binance state (one per asset, shared across all intervals) ────────────────

class BinanceState:
    """
    Real-time spot price + realized volatility from Binance trade stream.

    Shared across all intervals for a given asset — one WS connection per asset.
    The per-window open price is fetched from REST in MarketSession.run_market()
    because different intervals have different window boundaries.
    """

    def __init__(self, symbol: str):
        self._symbol = symbol          # e.g. "BTCUSDT"
        self._lock   = threading.Lock()
        self._price: float | None      = None
        self._vol_samples: deque       = deque(maxlen=VOL_SAMPLES)
        self._last_vol_ts: int         = 0
        threading.Thread(
            target=self._ws_loop, daemon=True, name=f"binance-{symbol.lower()}"
        ).start()

    # ── Public API ────────────────────────────────────────────────────────────

    @property
    def price(self) -> float | None:
        with self._lock:
            return self._price

    @property
    def vol_ann(self) -> float:
        with self._lock:
            return self._compute_vol()

    def fetch_window_open(self, interval_ts: int, binance_interval: str) -> float | None:
        """Fetch the true kline open price for the window starting at interval_ts."""
        try:
            r = requests.get(
                f"{BINANCE_REST}/klines",
                params={
                    "symbol":    self._symbol,
                    "interval":  binance_interval,
                    "startTime": interval_ts * 1000,
                    "limit":     1,
                },
                timeout=8,
            )
            r.raise_for_status()
            data = r.json()
            if data and len(data[0]) >= 2:
                return float(data[0][1])
        except Exception as e:
            log.warning("Binance klines (%s %s) failed: %s", self._symbol, binance_interval, e)
        return None

    # ── Internal ──────────────────────────────────────────────────────────────

    def _ws_loop(self) -> None:
        ws_url = f"wss://stream.binance.com:9443/ws/{self._symbol.lower()}@trade"
        while True:
            try:
                with connect(ws_url, open_timeout=10, close_timeout=2) as ws:
                    log.info("Binance WS connected: %s", self._symbol)
                    for raw in ws:
                        try:
                            msg = json.loads(raw)
                            if msg.get("e") != "trade":
                                continue
                            ts_ms = int(msg["T"])
                            price = float(msg["p"])
                            with self._lock:
                                self._price = price
                                self._update_vol(ts_ms, price)
                        except Exception:
                            pass
            except Exception as e:
                log.warning("Binance WS (%s) reconnect: %s — retry 2s", self._symbol, e)
                time.sleep(2)

    def _update_vol(self, ts_ms: int, price: float) -> None:
        minute = (ts_ms // 1000 // VOL_INTERVAL) * VOL_INTERVAL
        if minute != self._last_vol_ts:
            self._last_vol_ts = minute
            self._vol_samples.append(price)

    def _compute_vol(self) -> float:
        """Annualized vol from 1-min samples. Mirrors BinanceActor._compute_vol()."""
        samples = list(self._vol_samples)
        if len(samples) < 2:
            return FALLBACK_VOL
        log_returns = [
            math.log(samples[i] / samples[i - 1])
            for i in range(1, len(samples))
            if samples[i - 1] > 0 and samples[i] > 0
        ]
        if len(log_returns) < 2:
            return FALLBACK_VOL
        std = statistics.stdev(log_returns)
        return std * math.sqrt(525_960)   # 365.25 * 24 * 60 minutes per year


# Global cache — one BinanceState per asset, lazily created
_BINANCE: dict[str, BinanceState] = {}
_BINANCE_LOCK = threading.Lock()

def _get_binance(asset: str) -> BinanceState:
    symbol = BINANCE_SYMBOLS.get(asset)
    if not symbol:
        raise ValueError(f"No Binance symbol for asset: {asset}")
    with _BINANCE_LOCK:
        if asset not in _BINANCE:
            _BINANCE[asset] = BinanceState(symbol)
        return _BINANCE[asset]


# ── Live orderbook ────────────────────────────────────────────────────────────

class LiveBook:
    def __init__(self, token_id: str):
        self.token_id = token_id
        self._lock = threading.Lock()
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}

    def apply_snapshot(self, bids: list, asks: list):
        with self._lock:
            self._bids = {float(x["price"]): float(x["size"]) for x in bids if x.get("price") and x.get("size")}
            self._asks = {float(x["price"]): float(x["size"]) for x in asks if x.get("price") and x.get("size")}

    def apply_change(self, side: str, price: float, size: float | None):
        book = self._bids if side == "buy" else self._asks
        with self._lock:
            if not size or size <= 0: book.pop(price, None)
            else: book[price] = size

    def snapshot(self, depth: int = ORDER_BOOK_DEPTH) -> dict:
        with self._lock:
            bids = sorted(self._bids.items(), key=lambda x: x[0], reverse=True)
            asks = sorted(self._asks.items(), key=lambda x: x[0])
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        tb = sum(s for _, s in bids[:depth])
        ta = sum(s for _, s in asks[:depth])
        out: dict[str, Any] = {
            "best_bid":   best_bid or "", "best_ask":   best_ask or "",
            "midpoint":   round((best_bid + best_ask) / 2, 6) if best_bid and best_ask else "",
            "total_bid":  tb, "total_ask":  ta,
            "imbalance":  round(tb / (tb + ta), 4) if (tb + ta) > 0 else "",
            "bid_levels": len(bids), "ask_levels": len(asks),
        }
        for i in range(depth):
            out[f"bid_price_{i}"] = bids[i][0] if i < len(bids) else ""
            out[f"bid_size_{i}"]  = bids[i][1] if i < len(bids) else ""
            out[f"ask_price_{i}"] = asks[i][0] if i < len(asks) else ""
            out[f"ask_size_{i}"]  = asks[i][1] if i < len(asks) else ""
        return out


# ── WebSocket feed ────────────────────────────────────────────────────────────

class WSFeed:
    def __init__(self, up: LiveBook, dn: LiveBook, on_tick: Callable):
        self._books  = {up.token_id: up, dn.token_id: dn}
        self._up, self._dn = up, dn
        self._on_tick = on_tick
        self._stop   = threading.Event()
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self): self._stop.set()

    def _run(self):
        ids = [self._up.token_id, self._dn.token_id]
        while not self._stop.is_set():
            try:
                with connect(WS_URL, open_timeout=10, close_timeout=2) as ws:
                    ws.send(json.dumps({"type": "market", "assets_ids": ids}))
                    for raw in ws:
                        if self._stop.is_set(): return
                        if self._handle(raw):
                            self._on_tick(self._up, self._dn)
            except Exception as e:
                if not self._stop.is_set():
                    log.warning("WS reconnect (%s) — retry 2s", e)
                    time.sleep(2)

    def _handle(self, raw: str) -> bool:
        try: msgs = json.loads(raw)
        except: return False
        if not isinstance(msgs, list): msgs = [msgs]
        changed = False
        for msg in msgs:
            if not isinstance(msg, dict): continue
            etype = str(msg.get("event_type") or msg.get("type") or "").lower()
            tid   = str(msg.get("asset_id") or msg.get("token_id") or "")
            if etype == "book" or ("bids" in msg and "asks" in msg):
                b = self._books.get(tid)
                if b: b.apply_snapshot(msg.get("bids", []), msg.get("asks", [])); changed = True
            elif etype == "price_change":
                for chg in (msg.get("price_changes") or msg.get("changes") or [msg]):
                    if not isinstance(chg, dict): continue
                    ctid  = str(chg.get("asset_id") or chg.get("token_id") or tid or "")
                    b     = self._books.get(ctid)
                    side  = str(chg.get("side") or "").lower()
                    price = _flt(chg.get("price"))
                    size  = _flt(chg.get("size") or chg.get("new_size"))
                    if b and price is not None and side in ("buy","sell"):
                        b.apply_change(side, price, size); changed = True
        return changed


# ── Market session ────────────────────────────────────────────────────────────

class MarketSession:
    def __init__(self, asset: str, interval: str, interval_sec: int, fmt: str):
        self.asset        = asset
        self.interval     = interval
        self.interval_sec = interval_sec
        self.market_type  = f"{asset}-{interval}"
        self.fmt          = fmt                      # "parquet" or "csv"
        self.binance      = _get_binance(asset)
        self.bin_interval = BINANCE_INTERVAL[interval]

        d = os.path.join(DATA_DIR, asset, interval)
        os.makedirs(d, exist_ok=True)
        self.data_dir    = d
        self.tick_csv    = os.path.join(d, "orderbook_ticks.csv")
        self.outcome_csv = os.path.join(d, "market_outcomes.csv")
        self.ticks_dir   = os.path.join(d, "ticks")        # parquet per-window dir
        self.outcome_pq  = os.path.join(d, "market_outcomes.parquet")

        if fmt == "csv":
            _ensure(self.tick_csv,    TICK_FIELDS)
            _ensure(self.outcome_csv, OUTCOME_FIELDS)
        else:
            os.makedirs(self.ticks_dir, exist_ok=True)

    def _interval_ts(self, ts: float | None = None) -> int:
        return (int(ts or time.time()) // self.interval_sec) * self.interval_sec

    def run_market(self, interval_ts: int):
        end_ts = interval_ts + self.interval_sec
        slug   = _build_slug(self.asset, self.interval, interval_ts)
        log.info("[%s] %s", self.market_type,
                 datetime.fromtimestamp(interval_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))

        ev = _wait_for_market(slug)
        if not ev:
            log.error("[%s] Market not found: %s — skipping", self.market_type, slug)
            return
        cid, up_tok, dn_tok = _extract_tokens(ev)

        # Fetch the true window open from Binance klines (once per window)
        window_open = self.binance.fetch_window_open(interval_ts, self.bin_interval)
        if window_open is None:
            log.warning("[%s] Binance window open unavailable — fair_value will be blank", self.market_type)

        up_book = LiveBook(up_tok)
        dn_book = LiveBook(dn_tok)
        state   = {
            "up_open": None, "dn_open": None, "up_close": None,
            "dn_close": None, "ticks": 0,
            "last_write_sec": -1,
            "tick_outcome": None,
            "rows": [],          # buffered rows for parquet mode
        }

        # Background REST cache for price, spread, and last-trade
        rest      = {"up_price": "", "dn_price": "",
                     "up_spread": "", "dn_spread": "",
                     "up_last": "", "dn_last": ""}
        rest_stop = threading.Event()
        def _refresh():
            while not rest_stop.is_set():
                rest["up_price"]  = _fetch_price(up_tok)  or ""
                rest["dn_price"]  = _fetch_price(dn_tok)  or ""
                rest["up_spread"] = _fetch_spread(up_tok) or ""
                rest["dn_spread"] = _fetch_spread(dn_tok) or ""
                rest["up_last"]   = _fetch_last(up_tok)   or ""
                rest["dn_last"]   = _fetch_last(dn_tok)   or ""
                rest_stop.wait(REST_REFRESH_SEC)
        threading.Thread(target=_refresh, daemon=True).start()

        def on_tick(up: LiveBook, dn: LiveBook):
            now     = time.time()
            now_sec = int(now)

            us, ds    = up.snapshot(), dn.snapshot()
            up_mid    = us["midpoint"] or None
            dn_mid    = ds["midpoint"] or None
            secs_left = max(end_ts - now_sec, 0)

            if state["up_open"] is None and up_mid: state["up_open"] = up_mid
            if state["dn_open"] is None and dn_mid: state["dn_open"] = dn_mid
            state["up_close"] = up_mid
            state["dn_close"] = dn_mid

            if secs_left == 0 and state["tick_outcome"] is None:
                up_bid = _flt(us["best_bid"])
                dn_bid = _flt(ds["best_bid"])
                if up_bid is not None and up_bid >= 0.99:
                    state["tick_outcome"] = "UP"
                elif dn_bid is not None and dn_bid >= 0.99:
                    state["tick_outcome"] = "DOWN"

            if now_sec == state["last_write_sec"]:
                return
            state["last_write_sec"] = now_sec
            state["ticks"] += 1

            psum = round((up_mid or 0) + (dn_mid or 0), 4) if up_mid and dn_mid else ""

            # ── Binance / fair-value fields ────────────────────────────────
            spot   = self.binance.price
            vol    = self.binance.vol_ann
            fv     = ""
            gap    = ""
            if spot is not None and window_open is not None:
                fv  = round(_fair_value(spot, window_open, secs_left, vol), 6)
                if up_mid:
                    gap = round(fv - up_mid, 6)

            row: dict[str, Any] = {
                "timestamp_utc":     datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "unix_ts":           now_sec, "market_type": self.market_type,
                "market_slug":       slug,    "condition_id": cid,
                "interval_start_ts": interval_ts, "interval_end_ts": end_ts,
                "seconds_remaining": secs_left,
                "up_token_id": up_tok, "down_token_id": dn_tok,
                "up_best_bid": us["best_bid"], "up_best_ask": us["best_ask"],
                "up_midpoint": up_mid or "", "up_price": rest["up_price"], "up_spread": rest["up_spread"], "up_last_trade": rest["up_last"],
                "up_total_bid": us["total_bid"], "up_total_ask": us["total_ask"],
                "up_imbalance": us["imbalance"], "up_bid_lvls": us["bid_levels"], "up_ask_lvls": us["ask_levels"],
                "dn_best_bid": ds["best_bid"], "dn_best_ask": ds["best_ask"],
                "dn_midpoint": dn_mid or "", "dn_price": rest["dn_price"], "dn_spread": rest["dn_spread"], "dn_last_trade": rest["dn_last"],
                "dn_total_bid": ds["total_bid"], "dn_total_ask": ds["total_ask"],
                "dn_imbalance": ds["imbalance"], "dn_bid_lvls": ds["bid_levels"], "dn_ask_lvls": ds["ask_levels"],
                "implied_up": up_mid or "", "implied_dn": dn_mid or "", "price_sum": psum,
                # Binance / backtest fields
                "btc_price":         spot if spot is not None else "",
                "window_open_price": window_open if window_open is not None else "",
                "btc_vol_ann":       round(vol, 6),
                "fair_value":        fv,
                "poly_yes_price":    up_mid or "",
                "gap":               gap,
            }
            for i in range(ORDER_BOOK_DEPTH):
                for pfx, snap in (("up", us), ("dn", ds)):
                    row[f"{pfx}_bid_price_{i}"] = snap.get(f"bid_price_{i}", "")
                    row[f"{pfx}_bid_size_{i}"]  = snap.get(f"bid_size_{i}", "")
                    row[f"{pfx}_ask_price_{i}"] = snap.get(f"ask_price_{i}", "")
                    row[f"{pfx}_ask_size_{i}"]  = snap.get(f"ask_size_{i}", "")

            if self.fmt == "csv":
                _append(self.tick_csv, TICK_FIELDS, row)
            else:
                state["rows"].append(row)

        tick_path  = os.path.join(self.ticks_dir, f"{interval_ts}.parquet")
        last_flush = 0
        pq_writer  = ParquetWindowWriter(tick_path) if self.fmt == "parquet" else None

        feed = WSFeed(up_book, dn_book, on_tick)
        try:
            while time.time() < end_ts + 5 and not _stop_event.is_set():
                time.sleep(1)
                if pq_writer and state["rows"]:
                    now_sec = int(time.time())
                    if now_sec - last_flush >= FLUSH_INTERVAL:
                        n = pq_writer.flush(state["rows"])
                        last_flush = now_sec
                        log.info("[%s] flushed +%d rows → %s", self.market_type,
                                 n, os.path.relpath(tick_path, DATA_DIR))
        finally:
            feed.stop()
            rest_stop.set()
            if pq_writer:
                n = pq_writer.flush(state["rows"])   # write any rows since last flush
                pq_writer.close()
                if state["rows"]:
                    log.info("[%s] closed writer, total %d rows → %s",
                             self.market_type, len(state["rows"]),
                             os.path.relpath(tick_path, DATA_DIR))

        # ── Resolve outcome ───────────────────────────────────────────────
        outcome = "UNKNOWN"

        if state["tick_outcome"]:
            outcome = state["tick_outcome"]

        if outcome == "UNKNOWN":
            time.sleep(5)
            try:
                ev2    = _fetch_event(slug) or ev
                prices = json.loads(ev2["markets"][0].get("outcomePrices", "[0,0]"))
                outs   = json.loads(ev2["markets"][0].get("outcomes", "[]"))
                for i, p in enumerate(prices):
                    if float(p) >= 0.99:
                        lbl = outs[i].upper() if i < len(outs) else ""
                        outcome = "UP" if lbl in ("UP", "YES") else "DOWN"
            except Exception:
                pass

        if outcome == "UNKNOWN":
            up_c = state["up_close"]
            dn_c = state["dn_close"]
            if up_c is not None and up_c > 0.5:
                outcome = "UP"
            elif dn_c is not None and dn_c > 0.5:
                outcome = "DOWN"
            elif up_c is not None and dn_c is not None:
                outcome = "UP" if up_c >= dn_c else "DOWN"

        outcome_row = {
            "market_type": self.market_type, "market_slug": slug, "condition_id": cid,
            "interval_start_ts": interval_ts, "interval_end_ts": end_ts,
            "interval_start_utc": datetime.fromtimestamp(interval_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "interval_end_utc":   datetime.fromtimestamp(end_ts,       tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "up_token_id": up_tok, "down_token_id": dn_tok,
            "outcome": outcome, "up_open": state["up_open"] or "", "up_close": state["up_close"] or "",
            "dn_open": state["dn_open"] or "", "dn_close": state["dn_close"] or "",
            "total_ticks": state["ticks"],
            "resolved_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        }
        if self.fmt == "csv":
            _append(self.outcome_csv, OUTCOME_FIELDS, outcome_row)
        else:
            _append_outcomes_parquet(self.outcome_pq, outcome_row)

        log.info("✓ [%s] %s → %s  (%d ticks)", self.market_type, slug, outcome, state["ticks"])

    def run(self):
        log.info("[%s] Collector starting", self.market_type)
        while not _stop_event.is_set():
            now_ts = time.time()
            cur    = self._interval_ts(now_ts)
            end    = cur + self.interval_sec
            if end - now_ts < 10:
                time.sleep(end - now_ts + 2)
                continue
            self.run_market(cur)
            time.sleep(3)
        log.info("[%s] Collector stopped", self.market_type)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Polymarket multi-asset collector")
    p.add_argument("--assets",    nargs="+", default=ALL_ASSETS,
                   choices=ALL_ASSETS, metavar="ASSET")
    p.add_argument("--intervals", nargs="+", default=[iv for iv, _ in ALL_INTERVALS],
                   choices=[iv for iv, _ in ALL_INTERVALS], metavar="INTERVAL")
    p.add_argument("--format",    default="csv", choices=["parquet", "csv"],
                   help="Output format (default: csv)")
    args = p.parse_args()

    iv_map = dict(ALL_INTERVALS)
    sessions = [
        MarketSession(asset, iv, iv_map[iv], args.format)
        for asset in args.assets
        for iv in args.intervals
    ]

    log.info("Starting %d collectors: %s × %s  [format=%s]",
             len(sessions), args.assets, args.intervals, args.format)
    log.info("Data dir: %s", DATA_DIR)

    import signal

    def _handle_stop(sig, _frame):
        log.info("Signal %d received — flushing and stopping…", sig)
        _stop_event.set()

    signal.signal(signal.SIGINT,  _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGHUP,  _handle_stop)   # tmux kill-session sends SIGHUP

    threads = [
        threading.Thread(target=s.run, daemon=False,
                         name=f"{s.asset}-{s.interval}")
        for s in sessions
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log.info("All collectors stopped.")


if __name__ == "__main__":
    main()
