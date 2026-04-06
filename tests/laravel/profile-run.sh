#!/bin/bash
# =============================================================================
# Profile ACDC under sustained load
#
# Usage:
#   ./tests/laravel/profile-run.sh [duration_minutes]
#
# Default: 240 minutes (4 hours). Use a shorter duration for quick tests:
#   ./tests/laravel/profile-run.sh 5     # 5-minute smoke test
#   ./tests/laravel/profile-run.sh 60    # 1-hour run
# =============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_DIR"

DURATION_MIN=${1:-240}
DURATION_SECS=$((DURATION_MIN * 60))
MYSQL="docker exec -i acdc-mysql mysql -u root -proot --batch -N"
CONFIG="$PROJECT_DIR/tests/laravel/acdc-laravel.toml"
LOG_DIR="/tmp/acdc-profile-$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

echo "╔══════════════════════════════════════════════════╗"
echo "║  ACDC Sustained Load Profile                     ║"
echo "║  Duration: ${DURATION_MIN} minutes                          ║"
echo "║  Output:   ${LOG_DIR}  ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ─── 0. Build release ───────────────────────────────────────────────────────

echo "--- Building release binary..."
cargo build --release --quiet 2>&1
ACDC_BIN="./target/release/acdc"
echo "    Binary: $ACDC_BIN ($(du -h "$ACDC_BIN" | cut -f1))"
echo ""

# ─── 1. Reset database ──────────────────────────────────────────────────────

echo "--- Resetting laravel_app database..."
$MYSQL -e "DROP DATABASE IF EXISTS laravel_app;" 2>/dev/null
$MYSQL -e "CREATE DATABASE laravel_app CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" 2>/dev/null
$MYSQL laravel_app < $SCRIPT_DIR/schema.sql 2>/dev/null
TABLE_COUNT=$($MYSQL -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='laravel_app';" 2>/dev/null)
echo "    $TABLE_COUNT tables created"

# Seed baseline data so the load generator has something to update
$MYSQL laravel_app < $SCRIPT_DIR/workload.sql 2>/dev/null
echo "    Baseline data seeded"
echo ""

# ─── 2. Start ACDC ──────────────────────────────────────────────────────────

echo "--- Starting ACDC (release)..."
$ACDC_BIN "$CONFIG" > "$LOG_DIR/acdc.log" 2>&1 &
ACDC_PID=$!
echo "    PID: $ACDC_PID"

# Wait for snapshot + CDC
for i in $(seq 1 60); do
    if grep -q "Listening for binlog events" "$LOG_DIR/acdc.log" 2>/dev/null; then
        echo "    Snapshot + CDC ready (${i}s)"
        break
    fi
    if ! kill -0 $ACDC_PID 2>/dev/null; then
        echo "FAIL: ACDC exited during startup"
        cat "$LOG_DIR/acdc.log"
        exit 1
    fi
    sleep 1
done
echo ""

# ─── 3. Start resource monitor ──────────────────────────────────────────────

echo "--- Starting resource monitor..."
(
    echo "timestamp,rss_kb,vsz_kb,cpu_pct,threads,fd_count" > "$LOG_DIR/resources.csv"
    while kill -0 $ACDC_PID 2>/dev/null; do
        TS=$(date +%s)
        # RSS, VSZ, CPU from ps
        PS_LINE=$(ps -p $ACDC_PID -o rss=,vsz=,%cpu=,nlwp= 2>/dev/null || echo "0 0 0 0")
        RSS=$(echo "$PS_LINE" | awk '{print $1}')
        VSZ=$(echo "$PS_LINE" | awk '{print $2}')
        CPU=$(echo "$PS_LINE" | awk '{print $3}')
        THREADS=$(echo "$PS_LINE" | awk '{print $4}')
        # FD count
        FDS=$(ls /proc/$ACDC_PID/fd 2>/dev/null | wc -l || echo 0)
        echo "${TS},${RSS},${VSZ},${CPU},${THREADS},${FDS}" >> "$LOG_DIR/resources.csv"
        sleep 10
    done
) &
MONITOR_PID=$!
echo "    Monitor PID: $MONITOR_PID"
echo ""

# ─── 4. Start load generator ────────────────────────────────────────────────

echo "--- Starting sustained load (${DURATION_MIN} min)..."
echo "    Load log: $LOG_DIR/load.log"
echo ""
bash "$SCRIPT_DIR/sustained-load.sh" "$DURATION_SECS" > "$LOG_DIR/load.log" 2>&1 &
LOAD_PID=$!

# ─── 5. Live status display ─────────────────────────────────────────────────

# Report every 60s for long runs, every 15s for short runs
if [ "$DURATION_MIN" -le 10 ]; then
    REPORT_INTERVAL=15
else
    REPORT_INTERVAL=60
fi
echo "--- Live status (every ${REPORT_INTERVAL}s):"
echo ""
printf "%-10s  %8s  %8s  %6s  %6s  %10s  %s\n" \
    "TIME" "RSS_MB" "CPU%" "FDs" "THRDS" "EVENTS" "HEALTH"
printf "%-10s  %8s  %8s  %6s  %6s  %10s  %s\n" \
    "--------" "------" "----" "----" "-----" "--------" "------"

while kill -0 $LOAD_PID 2>/dev/null; do
    sleep $REPORT_INTERVAL

    # Check ACDC is still alive
    if ! kill -0 $ACDC_PID 2>/dev/null; then
        echo ""
        echo "FAIL: ACDC crashed during load test"
        echo "Last 20 lines of log:"
        tail -20 "$LOG_DIR/acdc.log"
        kill $LOAD_PID 2>/dev/null || true
        kill $MONITOR_PID 2>/dev/null || true
        exit 1
    fi

    # Collect metrics
    PS_LINE=$(ps -p $ACDC_PID -o rss=,%cpu=,nlwp= 2>/dev/null || echo "0 0 0")
    RSS_KB=$(echo "$PS_LINE" | awk '{print $1}')
    RSS_MB=$(echo "scale=1; $RSS_KB / 1024" | bc 2>/dev/null || echo "?")
    CPU=$(echo "$PS_LINE" | awk '{print $2}')
    THREADS=$(echo "$PS_LINE" | awk '{print $3}')
    FDS=$(ls /proc/$ACDC_PID/fd 2>/dev/null | wc -l || echo "?")

    HEALTH=$(curl -sf http://localhost:8080/health 2>/dev/null || echo '{}')
    EVENTS=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('events_processed','-'))" 2>/dev/null || echo "-")
    STATUS=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','-'))" 2>/dev/null || echo "-")

    printf "%-10s  %8s  %8s  %6s  %6s  %10s  %s\n" \
        "$(date +%H:%M:%S)" "$RSS_MB" "$CPU" "$FDS" "$THREADS" "$EVENTS" "$STATUS"
done

echo ""
echo "--- Load generation finished, waiting for final flush..."
sleep 15

# ─── 6. Shutdown ─────────────────────────────────────────────────────────────

echo "--- Shutting down ACDC..."
kill $ACDC_PID 2>/dev/null
for i in $(seq 1 20); do
    kill -0 $ACDC_PID 2>/dev/null || break
    sleep 1
done
kill -9 $ACDC_PID 2>/dev/null || true
wait $ACDC_PID 2>/dev/null || true
kill $MONITOR_PID 2>/dev/null || true
wait $MONITOR_PID 2>/dev/null || true

# ─── 7. Generate report ─────────────────────────────────────────────────────

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  Profile Report                                  ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ACDC log summary
CLEAN_LOG=$(sed 's/\x1b\[[0-9;]*m//g' "$LOG_DIR/acdc.log")
TOTAL_EVENTS=$(echo "$CLEAN_LOG" | grep -o 'total_events=[0-9]*' | tail -1 | cut -d= -f2)
COMMITTED=$(echo "$CLEAN_LOG" | grep -c "Committed to Iceberg" || true)
ERRORS=$(echo "$CLEAN_LOG" | grep -c " ERROR " || true)
WARNINGS=$(echo "$CLEAN_LOG" | grep -c " WARN " || true)

echo "ACDC Summary:"
echo "    Total CDC events:   ${TOTAL_EVENTS:-0}"
echo "    Iceberg commits:    $COMMITTED"
echo "    Errors:             $ERRORS"
echo "    Warnings:           $WARNINGS"
echo ""

# Resource summary from CSV
if [ -f "$LOG_DIR/resources.csv" ]; then
    echo "Resource Usage (from $(wc -l < "$LOG_DIR/resources.csv") samples):"
    python3 -c "
import csv, sys
rows = list(csv.DictReader(open('$LOG_DIR/resources.csv')))
if not rows:
    print('    No data collected')
    sys.exit()
rss = [int(r['rss_kb']) for r in rows if r['rss_kb'] != '0']
cpu = [float(r['cpu_pct']) for r in rows if r['cpu_pct'] != '0']
fds = [int(r['fd_count']) for r in rows if r['fd_count'] != '0']
if rss:
    print(f'    RSS (MB):  min={min(rss)/1024:.1f}  max={max(rss)/1024:.1f}  avg={sum(rss)/len(rss)/1024:.1f}')
if cpu:
    print(f'    CPU (%):   min={min(cpu):.1f}  max={max(cpu):.1f}  avg={sum(cpu)/len(cpu):.1f}')
if fds:
    print(f'    FDs:       min={min(fds)}  max={max(fds)}  avg={sum(fds)/len(fds):.0f}')
# Check for memory leak (linear growth)
if len(rss) > 10:
    first_q = rss[:len(rss)//4]
    last_q = rss[-len(rss)//4:]
    first_avg = sum(first_q) / len(first_q)
    last_avg = sum(last_q) / len(last_q)
    growth = (last_avg - first_avg) / first_avg * 100 if first_avg > 0 else 0
    if growth > 50:
        print(f'    ⚠ POSSIBLE MEMORY LEAK: RSS grew {growth:.0f}% from first to last quarter')
    else:
        print(f'    Memory growth: {growth:.1f}% (first→last quarter)')
" 2>/dev/null || echo "    (python3 not available for analysis)"
fi
echo ""

# MySQL row counts
echo "Final MySQL Row Counts:"
for t in migrations password_reset_tokens sessions cache cache_locks jobs job_batches failed_jobs personal_access_tokens users teams products orders order_items notifications activity_log; do
    COUNT=$($MYSQL -e "SELECT COUNT(*) FROM laravel_app.$t;" 2>/dev/null || echo "?")
    printf "    %-30s %s\n" "$t" "$COUNT"
done
echo ""

# Load generator summary
if [ -f "$LOG_DIR/load.log" ]; then
    echo "Load Generator Summary:"
    tail -20 "$LOG_DIR/load.log" | grep -E "Stats:|events|complete" | tail -5 | sed 's/^/    /'
fi
echo ""

echo "Artifacts:"
echo "    ACDC log:      $LOG_DIR/acdc.log"
echo "    Resource CSV:  $LOG_DIR/resources.csv"
echo "    Load log:      $LOG_DIR/load.log"
echo ""

if [ "$ERRORS" -gt 0 ]; then
    echo "⚠ ACDC had $ERRORS errors — check $LOG_DIR/acdc.log"
    exit 1
fi

echo "✓ Profile run complete — no errors"
