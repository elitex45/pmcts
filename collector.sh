#!/usr/bin/env bash
# collector.sh — self-contained launcher for the Polymarket data collector
#
# Everything lives inside this folder: venv, logs, and data.
#
# Usage:
#   ./collector.sh start     Create venv (first time), then launch in tmux
#   ./collector.sh stop      Kill the session
#   ./collector.sh restart   Stop + start
#   ./collector.sh attach    Re-attach to running session
#   ./collector.sh status    Show running status + data stats
#   ./collector.sh logs      Tail live log output
#   ./collector.sh setup     Create venv + install deps (runs automatically on start)

set -euo pipefail

SESSION="data-collector"
DIR="$(cd "$(dirname "$0")" && pwd)"   # always pypt/collector/
VENV="$DIR/.venv"
PYTHON="$VENV/bin/python"
PIP="$VENV/bin/pip"
COLLECTOR="$DIR/collect.py"
DATA_DIR="$DIR/data"
LOG="$DIR/collector.log"

_green()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
_yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
_red()    { printf '\033[0;31m%s\033[0m\n' "$*"; }
_bold()   { printf '\033[1m%s\033[0m\n' "$*"; }

_running() { tmux has-session -t "$SESSION" 2>/dev/null; }

# ── setup: create local venv + install deps ───────────────────────────────────
cmd_setup() {
    _bold "=== Collector setup ==="

    # Find a suitable Python (3.9+ required for zoneinfo)
    local py=""
    for candidate in python3 python3.12 python3.11 python3.10 python3.9; do
        if command -v "$candidate" &>/dev/null; then
            local ver
            ver=$("$candidate" -c "import sys; print(sys.version_info >= (3,9))" 2>/dev/null || echo "False")
            if [[ "$ver" == "True" ]]; then
                py=$(command -v "$candidate")
                break
            fi
        fi
    done

    if [[ -z "$py" ]]; then
        _red "Python 3.9+ not found. Install it with: brew install python3"
        exit 1
    fi
    echo "  Using Python: $py ($("$py" --version))"

    if [[ ! -d "$VENV" ]]; then
        echo "  Creating venv at $VENV ..."
        "$py" -m venv "$VENV"
        _green "  Venv created."
    else
        _green "  Venv exists: $VENV"
    fi

    echo "  Installing dependencies..."
    "$PIP" install --upgrade pip -q
    "$PIP" install "requests>=2.31.0" "websockets>=12.0" "pandas>=2.0.0" "pyarrow>=14.0.0" -q
    _green "  Dependencies installed."
    echo ""
    _green "Setup complete. Run: ./collector.sh start"
}

# ── start ─────────────────────────────────────────────────────────────────────
cmd_start() {
    if _running; then
        _yellow "Collector already running. Use: ./collector.sh attach"
        exit 1
    fi

    # Auto-setup if venv missing
    if [[ ! -f "$PYTHON" ]]; then
        _yellow "Venv not found — running setup first..."
        echo ""
        cmd_setup
        echo ""
    fi

    tmux new-session -d -s "$SESSION" -x 200 -y 50 -c "$DIR"
    tmux send-keys -t "$SESSION" \
        "source '$VENV/bin/activate' && python '$COLLECTOR' ${EXTRA_ARGS[*]} 2>&1 | tee '$LOG'" \
        Enter

    _green "Collector started."
    echo "  Attach : ./collector.sh attach"
    echo "  Logs   : ./collector.sh logs"
    echo "  Data   : $DATA_DIR"
}

cmd_stop() {
    if _running; then
        _yellow "Sending stop signal — waiting for clean flush…"
        tmux send-keys -t "$SESSION" C-c ""   # SIGINT → Python handler sets _stop_event
        local deadline=$(( $(date +%s) + 30 ))
        while _running && [[ $(date +%s) -lt $deadline ]]; do
            sleep 1
        done
        if _running; then
            tmux kill-session -t "$SESSION" 2>/dev/null || true  # force if needed
        fi
        _green "Stopped."
    else
        _yellow "Not running."
    fi
}

cmd_restart() { cmd_stop; sleep 1; cmd_start; }

cmd_attach() {
    if _running; then
        tmux attach -t "$SESSION"
    else
        _red "Not running. Use: ./collector.sh start"
        exit 1
    fi
}

cmd_status() {
    _bold "=== Collector status ==="
    if _running; then
        _green "  RUNNING  (session: $SESSION)"
    else
        echo "  stopped"
    fi

    echo ""
    _bold "  Data summary (Polymarket orderbook ticks):"

    if [[ ! -d "$DATA_DIR" ]]; then
        echo "  No data yet — collector may still be discovering first markets."
        return
    fi

    printf "  %-6s  %-5s  %8s ticks   %6s outcomes  %s\n" "ASSET" "INTV" "TICKS" "OUTC" "FORMAT"
    printf "  %s\n" "$(printf '─%.0s' {1..60})"
    for asset in btc eth sol xrp bnb; do
        for iv in 5m 15m 1h 4h 1d; do
            d="$DATA_DIR/$asset/$iv"
            [[ -d "$d" ]] || continue
            ticks=0; outcomes=0; fmt="csv"
            [[ -f "$d/orderbook_ticks.csv"  ]] && ticks=$(( $(wc -l < "$d/orderbook_ticks.csv") - 1 ))
            [[ -f "$d/market_outcomes.csv"  ]] && outcomes=$(( $(wc -l < "$d/market_outcomes.csv") - 1 ))
            if [[ -d "$d/ticks" ]]; then
                pq_count=$(find "$d/ticks" -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
                ticks="$pq_count windows"; fmt="parquet"
            fi
            [[ -f "$d/market_outcomes.parquet" ]] && outcomes="(pq)" && fmt="parquet"
            printf "  %-6s  %-5s  %8s         %6s  %s\n" "$asset" "$iv" "$ticks" "$outcomes" "$fmt"
        done
    done
}

cmd_logs() {
    if [[ -f "$LOG" ]]; then
        tail -f "$LOG"
    else
        _yellow "No log file yet. Start the collector first."
    fi
}

CMD="${1:-start}"
shift 2>/dev/null || true
EXTRA_ARGS=("$@")   # remaining args forwarded to collect.py (e.g. --format csv)

case "$CMD" in
    start)   cmd_start   ;;
    stop)    cmd_stop    ;;
    restart) cmd_restart ;;
    attach)  cmd_attach  ;;
    status)  cmd_status  ;;
    logs)    cmd_logs    ;;
    setup)   cmd_setup   ;;
    help|-h|--help)
        cat <<EOF
Usage: ./collector.sh <command>

Commands:
  start    Create venv if needed, then launch collector in tmux
  stop     Kill the collector
  restart  Stop + start
  attach   Re-attach to running session
  status   Show session state + per-market tick/outcome counts
  logs     Tail collector.log
  setup    Create .venv + install deps (runs automatically on first start)

Data layout (parquet, default):
  data/{asset}/{interval}/ticks/{ts}.parquet    (one file per window)
  data/{asset}/{interval}/market_outcomes.parquet

Data layout (csv, --format csv):
  data/{asset}/{interval}/orderbook_ticks.csv   (1 row/sec WS book snapshot)
  data/{asset}/{interval}/market_outcomes.csv   (one row per closed market)

Assets:    btc, eth, sol, xrp, bnb
Intervals: 5m, 15m, 1h, 4h, 1d

Run subset:
  python collect.py --assets btc eth --intervals 5m 15m
  python collect.py --format csv
EOF
        ;;
    *)
        _red "Unknown command: $CMD"
        echo "Run: ./collector.sh help"
        exit 1
        ;;
esac
