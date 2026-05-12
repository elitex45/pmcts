"""
Collect Chainlink BTC/USD price every second via Polymarket RTDS WebSocket.

Connects to wss://ws-live-data.polymarket.com, subscribes to the
crypto_prices_chainlink topic, filters for btc/usd, and writes each
price tick to a daily CSV file (1 row per second).

Output: data/btc_chainlink_price_YYYY-MM-DD.csv (UTC date)

Run:  python collect_btc_price.py
"""

import csv
import json
import logging
import os
import time
from datetime import datetime, timezone

from websockets.sync.client import connect

WS_URL = "wss://ws-live-data.polymarket.com"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
PING_INTERVAL = 5  # seconds

CSV_FIELDS = [
    "timestamp_utc",
    "unix_ts_ms",
    "btc_price_usd",
    "btc_price_full",
    "source",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def csv_path_for_ts(ts_sec: float) -> str:
    """Return the daily CSV path for the given unix timestamp (UTC date)."""
    date_str = datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(DATA_DIR, f"btc_chainlink_price_{date_str}.csv")


def append_row(path: str, row: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    new_file = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow(row)


def subscribe_msg():
    return json.dumps({
        "action": "subscribe",
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "*",
            }
        ],
    })


def run():
    os.makedirs(DATA_DIR, exist_ok=True)
    log.info("Starting Chainlink BTC/USD price collector")
    log.info("Writing daily CSVs under %s", DATA_DIR)

    while True:
        try:
            with connect(WS_URL, open_timeout=10, close_timeout=5) as ws:
                ws.send(subscribe_msg())
                log.info("Connected & subscribed to crypto_prices_chainlink")

                last_ping = time.time()
                last_write_sec = -1

                for raw in ws:
                    # Send PING to keep connection alive
                    now = time.time()
                    if now - last_ping >= PING_INTERVAL:
                        ws.send("PING")
                        last_ping = now

                    # Parse message
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue

                    if msg.get("topic") != "crypto_prices_chainlink":
                        continue

                    payload = msg.get("payload")
                    if not payload:
                        continue

                    # Filter for btc/usd only
                    if payload.get("symbol") != "btc/usd":
                        continue

                    price = payload.get("value")
                    full_price = payload.get("full_accuracy_value", "")
                    ts_ms = payload.get("timestamp")
                    if price is None or ts_ms is None:
                        continue

                    # Throttle to 1 row per second
                    ts_sec = int(ts_ms / 1000)
                    if ts_sec == last_write_sec:
                        continue
                    last_write_sec = ts_sec

                    ts_utc = datetime.fromtimestamp(
                        ts_ms / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S")

                    row = {
                        "timestamp_utc": ts_utc,
                        "unix_ts_ms": int(ts_ms),
                        "btc_price_usd": price,
                        "btc_price_full": full_price,
                        "source": "chainlink",
                    }
                    append_row(csv_path_for_ts(ts_sec), row)
                    log.info("BTC $%.2f  @ %s", price, ts_utc)

        except KeyboardInterrupt:
            log.info("Stopped by user")
            break
        except Exception as e:
            log.warning("Connection lost (%s) — reconnecting in 3s", e)
            time.sleep(3)


if __name__ == "__main__":
    run()
