#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/../.."

# ─── Config ──────────────────────────────────────────────────────────────────

MYSQL="docker exec -i acdc-mysql mysql -u root -proot"
ACDC_BIN="./target/debug/acdc"
CONFIG="tests/laravel/acdc-laravel.toml"
TABLES=(migrations password_reset_tokens sessions cache cache_locks jobs job_batches failed_jobs personal_access_tokens users teams products orders order_items notifications activity_log)
FLUSH_INTERVAL=10  # seconds — shorter for testing

echo "============================================"
echo "  ACDC Laravel Integration Test"
echo "============================================"
echo ""

# ─── 0. Build ────────────────────────────────────────────────────────────────

echo "--- Building ACDC..."
cargo build --quiet 2>&1

# ─── 1. Setup database ──────────────────────────────────────────────────────

echo "--- Creating laravel_app database..."
$MYSQL -e "CREATE DATABASE IF NOT EXISTS laravel_app CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" 2>/dev/null

echo "--- Loading schema..."
$MYSQL laravel_app < tests/laravel/schema.sql 2>/dev/null

echo "--- Verifying tables..."
TABLE_COUNT=$($MYSQL -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='laravel_app';" 2>/dev/null)
echo "    Found $TABLE_COUNT tables"
if [ "$TABLE_COUNT" -lt 16 ]; then
    echo "FAIL: Expected 16 tables, got $TABLE_COUNT"
    exit 1
fi

# ─── 2. Start ACDC ──────────────────────────────────────────────────────────

echo "--- Starting ACDC (snapshot + CDC)..."
$ACDC_BIN "$CONFIG" > /tmp/acdc-laravel-test.log 2>&1 &
ACDC_PID=$!
echo "    PID: $ACDC_PID"

# Wait for snapshot to complete
echo "--- Waiting for snapshot..."
for i in $(seq 1 30); do
    if grep -q "Listening for binlog events" /tmp/acdc-laravel-test.log 2>/dev/null; then
        echo "    Snapshot complete (${i}s)"
        break
    fi
    if ! kill -0 $ACDC_PID 2>/dev/null; then
        echo "FAIL: ACDC exited during snapshot"
        cat /tmp/acdc-laravel-test.log
        exit 1
    fi
    sleep 1
done

# Verify caught up
sleep 2
if ! grep -q "CDC caught up" /tmp/acdc-laravel-test.log 2>/dev/null; then
    echo "    Warning: CDC not yet caught up"
fi

# ─── 3. Run Laravel workload ────────────────────────────────────────────────

echo "--- Running Laravel workload..."
$MYSQL laravel_app < tests/laravel/workload.sql 2>/dev/null
echo "    Workload complete"

# ─── 4. Wait for CDC flush ──────────────────────────────────────────────────

echo "--- Waiting for CDC flush (${FLUSH_INTERVAL}s + buffer)..."
sleep $((FLUSH_INTERVAL + 5))

# ─── 5. Check health ────────────────────────────────────────────────────────

echo "--- Health check..."
HEALTH=$(curl -sf http://localhost:8080/health 2>/dev/null || echo '{"status":"unreachable"}')
STATUS=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','unknown'))" 2>/dev/null)
EVENTS=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('events_processed',0))" 2>/dev/null)
echo "    Status: $STATUS"
echo "    Events processed: $EVENTS"

if [ "$STATUS" != "ok" ]; then
    echo "FAIL: Health status is $STATUS"
    cat /tmp/acdc-laravel-test.log
    kill $ACDC_PID 2>/dev/null; wait $ACDC_PID 2>/dev/null
    exit 1
fi

# ─── 6. Verify row counts in MySQL ──────────────────────────────────────────

echo "--- Verifying MySQL row counts..."
for t in "${TABLES[@]}"; do
    COUNT=$($MYSQL -N -e "SELECT COUNT(*) FROM laravel_app.$t;" 2>/dev/null)
    echo "    $t: $COUNT rows"
done

# ─── 7. Graceful shutdown ───────────────────────────────────────────────────

echo "--- Shutting down ACDC..."
kill $ACDC_PID 2>/dev/null
for i in $(seq 1 15); do
    if ! kill -0 $ACDC_PID 2>/dev/null; then
        echo "    Exited after ${i}s"
        break
    fi
    sleep 1
done
kill -9 $ACDC_PID 2>/dev/null || true
wait $ACDC_PID 2>/dev/null || true

# ─── 8. Verify logs ─────────────────────────────────────────────────────────

echo "--- Log summary..."
# Strip ANSI codes for reliable grep
CLEAN_LOG=$(sed 's/\x1b\[[0-9;]*m//g' /tmp/acdc-laravel-test.log)
TOTAL_EVENTS=$(echo "$CLEAN_LOG" | grep -o 'total_events=[0-9]*' | tail -1 | cut -d= -f2)
COMMITTED=$(echo "$CLEAN_LOG" | grep -c "Committed to Iceberg" || true)
ERRORS=$(echo "$CLEAN_LOG" | grep -c " ERROR " || true)
WARNINGS=$(echo "$CLEAN_LOG" | grep -c " WARN " || true)

echo "    Total events: ${TOTAL_EVENTS:-0}"
echo "    Iceberg commits: $COMMITTED"
echo "    Errors: $ERRORS"
echo "    Warnings: $WARNINGS"

if [ "$ERRORS" -gt 0 ]; then
    echo ""
    echo "--- ERRORS found in log:"
    grep " ERROR " /tmp/acdc-laravel-test.log
fi

if [ "${TOTAL_EVENTS:-0}" -lt 10 ]; then
    echo "FAIL: Expected at least 10 total events, got ${TOTAL_EVENTS:-0}"
    exit 1
fi

echo ""
echo "============================================"
echo "  PASS - Laravel integration test complete"
echo "============================================"
echo ""
echo "Full log: /tmp/acdc-laravel-test.log"
