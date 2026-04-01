"""
Polymarket multi-asset, multi-interval orderbook collector.

One thread per (asset, interval) pair — each discovers the current Polymarket
up/down market, streams WS tick data, then rotates to the next interval.

Data layout:
  data/{asset}/{interval}/orderbook_ticks.csv   — tick on every WS book change
  data/{asset}/{interval}/market_outcomes.csv   — one row per closed market

Slug formats:
  5m / 15m / 4h  →  {asset}-updown-{interval}-{ts}
                    e.g. btc-updown-5m-1774466400
  1h             →  {full_name}-up-or-down-{month}-{day}-{year}-{hour}am/pm-et
                    e.g. bitcoin-up-or-down-april-1-2026-6am-et
  1d             →  {full_name}-up-or-down-on-{month}-{day}-{year}
                    e.g. bitcoin-up-or-down-on-april-1-2026

Run: python collect.py [--assets btc eth] [--intervals 5m 15m]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import threading
import time
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

CLOB_HOST        = "https://clob.polymarket.com"
GAMMA_HOST       = "https://gamma-api.polymarket.com"
WS_URL           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ORDER_BOOK_DEPTH = 10
REST_REFRESH_SEC = 30
DATA_DIR         = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ── CSV schema ────────────────────────────────────────────────────────────────

def _tick_fields(depth: int = ORDER_BOOK_DEPTH) -> list[str]:
    f = ["timestamp_utc", "unix_ts", "market_type", "market_slug", "condition_id",
         "interval_start_ts", "interval_end_ts", "seconds_remaining",
         "up_token_id", "down_token_id",
         "up_best_bid", "up_best_ask", "up_midpoint", "up_spread", "up_last_trade",
         "up_total_bid", "up_total_ask", "up_imbalance", "up_bid_lvls", "up_ask_lvls",
         "dn_best_bid", "dn_best_ask", "dn_midpoint", "dn_spread", "dn_last_trade",
         "dn_total_bid", "dn_total_ask", "dn_imbalance", "dn_bid_lvls", "dn_ask_lvls",
         "implied_up", "implied_dn", "price_sum"]
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
    def __init__(self, asset: str, interval: str, interval_sec: int):
        self.asset        = asset
        self.interval     = interval
        self.interval_sec = interval_sec
        self.market_type  = f"{asset}-{interval}"
        d = os.path.join(DATA_DIR, asset, interval)
        os.makedirs(d, exist_ok=True)
        self.tick_csv    = os.path.join(d, "orderbook_ticks.csv")
        self.outcome_csv = os.path.join(d, "market_outcomes.csv")
        _ensure(self.tick_csv,    TICK_FIELDS)
        _ensure(self.outcome_csv, OUTCOME_FIELDS)

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

        up_book = LiveBook(up_tok)
        dn_book = LiveBook(dn_tok)
        state   = {
            "up_open": None, "dn_open": None, "up_close": None,
            "dn_close": None, "ticks": 0,
            "last_write_sec": -1,   # unix second of last CSV write (1s throttle)
            "tick_outcome": None,   # set when last-second bid hits ≥ 0.99
        }

        # Background REST cache for spread + last-trade
        rest      = {"up_spread": "", "dn_spread": "", "up_last": "", "dn_last": ""}
        rest_stop = threading.Event()
        def _refresh():
            while not rest_stop.is_set():
                rest["up_spread"] = _fetch_spread(up_tok) or ""
                rest["dn_spread"] = _fetch_spread(dn_tok) or ""
                rest["up_last"]   = _fetch_last(up_tok)   or ""
                rest["dn_last"]   = _fetch_last(dn_tok)   or ""
                rest_stop.wait(REST_REFRESH_SEC)
        threading.Thread(target=_refresh, daemon=True).start()

        def on_tick(up: LiveBook, dn: LiveBook):
            now     = time.time()
            now_sec = int(now)

            # 1-second throttle: update book state on every WS message but only
            # write one row per second (the latest snapshot for that second).
            us, ds    = up.snapshot(), dn.snapshot()
            up_mid    = us["midpoint"] or None
            dn_mid    = ds["midpoint"] or None
            secs_left = max(end_ts - now_sec, 0)

            # Always track open / close prices regardless of write throttle
            if state["up_open"] is None and up_mid: state["up_open"] = up_mid
            if state["dn_open"] is None and dn_mid: state["dn_open"] = dn_mid
            state["up_close"] = up_mid
            state["dn_close"] = dn_mid

            # Capture outcome from the last second's tick data.
            # Winning side's best bid jumps to ≥ 0.99 at market close.
            if secs_left == 0 and state["tick_outcome"] is None:
                up_bid = _flt(us["best_bid"])
                dn_bid = _flt(ds["best_bid"])
                if up_bid is not None and up_bid >= 0.99:
                    state["tick_outcome"] = "UP"
                elif dn_bid is not None and dn_bid >= 0.99:
                    state["tick_outcome"] = "DOWN"

            # Skip write if already wrote this second
            if now_sec == state["last_write_sec"]:
                return
            state["last_write_sec"] = now_sec
            state["ticks"] += 1

            psum = round((up_mid or 0) + (dn_mid or 0), 4) if up_mid and dn_mid else ""
            row: dict[str, Any] = {
                "timestamp_utc":     datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "unix_ts":           now_sec, "market_type": self.market_type,
                "market_slug":       slug,    "condition_id": cid,
                "interval_start_ts": interval_ts, "interval_end_ts": end_ts,
                "seconds_remaining": secs_left,
                "up_token_id": up_tok, "down_token_id": dn_tok,
                "up_best_bid": us["best_bid"], "up_best_ask": us["best_ask"],
                "up_midpoint": up_mid or "", "up_spread": rest["up_spread"], "up_last_trade": rest["up_last"],
                "up_total_bid": us["total_bid"], "up_total_ask": us["total_ask"],
                "up_imbalance": us["imbalance"], "up_bid_lvls": us["bid_levels"], "up_ask_lvls": us["ask_levels"],
                "dn_best_bid": ds["best_bid"], "dn_best_ask": ds["best_ask"],
                "dn_midpoint": dn_mid or "", "dn_spread": rest["dn_spread"], "dn_last_trade": rest["dn_last"],
                "dn_total_bid": ds["total_bid"], "dn_total_ask": ds["total_ask"],
                "dn_imbalance": ds["imbalance"], "dn_bid_lvls": ds["bid_levels"], "dn_ask_lvls": ds["ask_levels"],
                "implied_up": up_mid or "", "implied_dn": dn_mid or "", "price_sum": psum,
            }
            for i in range(ORDER_BOOK_DEPTH):
                for pfx, snap in (("up", us), ("dn", ds)):
                    row[f"{pfx}_bid_price_{i}"] = snap.get(f"bid_price_{i}", "")
                    row[f"{pfx}_bid_size_{i}"]  = snap.get(f"bid_size_{i}", "")
                    row[f"{pfx}_ask_price_{i}"] = snap.get(f"ask_price_{i}", "")
                    row[f"{pfx}_ask_size_{i}"]  = snap.get(f"ask_size_{i}", "")
            _append(self.tick_csv, TICK_FIELDS, row)

        feed = WSFeed(up_book, dn_book, on_tick)
        try:
            while time.time() < end_ts + 5:
                time.sleep(1)
        finally:
            feed.stop()
            rest_stop.set()

        # Resolve outcome — three tiers, most-reliable first
        outcome = "UNKNOWN"

        # Tier 1: last-second tick — captured inside on_tick when secs_left == 0.
        # Winning side's best bid hits ≥ 0.99 at the moment the market resolves.
        if state["tick_outcome"]:
            outcome = state["tick_outcome"]

        # Tier 2: Polymarket REST outcomePrices (wait for settlement if tick missed it)
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

        # Tier 3: closing midpoint — side trading above 0.5 was winning at close
        if outcome == "UNKNOWN":
            up_c = state["up_close"]
            dn_c = state["dn_close"]
            if up_c is not None and up_c > 0.5:
                outcome = "UP"
            elif dn_c is not None and dn_c > 0.5:
                outcome = "DOWN"
            elif up_c is not None and dn_c is not None:
                outcome = "UP" if up_c >= dn_c else "DOWN"

        _append(self.outcome_csv, OUTCOME_FIELDS, {
            "market_type": self.market_type, "market_slug": slug, "condition_id": cid,
            "interval_start_ts": interval_ts, "interval_end_ts": end_ts,
            "interval_start_utc": datetime.fromtimestamp(interval_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "interval_end_utc":   datetime.fromtimestamp(end_ts,       tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "up_token_id": up_tok, "down_token_id": dn_tok,
            "outcome": outcome, "up_open": state["up_open"] or "", "up_close": state["up_close"] or "",
            "dn_open": state["dn_open"] or "", "dn_close": state["dn_close"] or "",
            "total_ticks": state["ticks"],
            "resolved_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        })
        log.info("✓ [%s] %s → %s  (%d ticks)", self.market_type, slug, outcome, state["ticks"])

    def run(self):
        log.info("[%s] Collector starting", self.market_type)
        while True:
            now_ts = time.time()
            cur    = self._interval_ts(now_ts)
            end    = cur + self.interval_sec
            if end - now_ts < 10:
                time.sleep(end - now_ts + 2)
                continue
            self.run_market(cur)
            time.sleep(3)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Polymarket multi-asset collector")
    p.add_argument("--assets",    nargs="+", default=ALL_ASSETS,
                   choices=ALL_ASSETS, metavar="ASSET")
    p.add_argument("--intervals", nargs="+", default=[iv for iv, _ in ALL_INTERVALS],
                   choices=[iv for iv, _ in ALL_INTERVALS], metavar="INTERVAL")
    args = p.parse_args()

    iv_map = dict(ALL_INTERVALS)
    sessions = [
        MarketSession(asset, iv, iv_map[iv])
        for asset in args.assets
        for iv in args.intervals
    ]

    log.info("Starting %d collectors: %s × %s",
             len(sessions), args.assets, args.intervals)
    log.info("Data dir: %s", DATA_DIR)

    threads = [
        threading.Thread(target=s.run, daemon=False,
                         name=f"{s.asset}-{s.interval}")
        for s in sessions
    ]
    for t in threads:
        t.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        log.info("Stopping…")


if __name__ == "__main__":
    main()
