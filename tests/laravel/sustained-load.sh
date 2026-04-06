#!/bin/bash
# =============================================================================
# Sustained load generator — simulates medium-usage Laravel app over 4 hours
#
# Target profile (~1.5 events/sec average, bursty):
#   - Session touches:  every 1-3s   (~1,200/hr)
#   - Cache put/delete: every 3-8s   (~600/hr)
#   - Activity log:     every 5-15s  (~400/hr)
#   - Token touch:      every 10-20s (~300/hr)
#   - Order lifecycle:  every 20-60s (~120/hr)
#   - User registration: every 60-180s (~40/hr)
#   - Notification:     every 30-90s  (~80/hr)
#   - Job dispatch:     every 10-30s  (~240/hr)
#   - Bulk burst:       every 15-30min
#
# Total: ~3,000 events/hr ≈ 12,000 over 4 hours
# =============================================================================
set -uo pipefail

DURATION_SECS=${1:-14400}  # default 4 hours, override with first arg
MYSQL="docker exec -i acdc-mysql mysql -u root -proot --batch -N laravel_app"
export MYSQL

START=$(date +%s)
END=$((START + DURATION_SECS))

# Counters
declare -A OPS
for op in session cache activity token order user notification job burst; do
    OPS[$op]=0
done
TOTAL=0
ERRORS=0

# Sequence counters
USER_SEQ=100
ORDER_SEQ=1000
NOTIF_SEQ=1000

rand_between() { echo $(( $1 + RANDOM % ($2 - $1 + 1) )); }

log() { echo "[$(date '+%H:%M:%S')] $*"; }

sql() {
    if ! echo "$1" | $MYSQL 2>/dev/null; then
        ((ERRORS++))
    fi
    ((TOTAL++))
}

report() {
    local now=$(date +%s)
    local elapsed=$(( now - START ))
    local rate=$(echo "scale=1; $TOTAL / ($elapsed + 1)" | bc 2>/dev/null || echo "?")
    log "── Stats: ${TOTAL} events (${rate}/s) | errors=${ERRORS} | elapsed=${elapsed}s"
    for op in "${!OPS[@]}"; do
        printf "    %-15s %d\n" "$op" "${OPS[$op]}"
    done | sort
}

# ─── Tick functions ──────────────────────────────────────────────────────────

do_session_touch() {
    local uid=$(rand_between 1 5)
    local sid="session-$(printf '%032x' $RANDOM$RANDOM)"
    # Upsert pattern: Laravel's session handler does INSERT ... ON DUPLICATE KEY UPDATE
    sql "INSERT INTO sessions (id, user_id, ip_address, user_agent, payload, last_activity)
         VALUES ('${sid}', ${uid}, '10.0.0.$(rand_between 1 254)', 'Mozilla/5.0', 'payload-$(date +%s)', UNIX_TIMESTAMP())
         ON DUPLICATE KEY UPDATE last_activity = UNIX_TIMESTAMP(), payload = VALUES(payload);"
    ((OPS[session]++))
}

do_cache_op() {
    local op=$(rand_between 1 3)
    local key="laravel_cache_key_$(rand_between 1 50)"
    case $op in
        1|2) # put
            sql "INSERT INTO cache (\`key\`, value, expiration)
                 VALUES ('${key}', 's:$(rand_between 10 500):\"cached-data-$(date +%s%N)\";', UNIX_TIMESTAMP() + $(rand_between 60 86400))
                 ON DUPLICATE KEY UPDATE value = VALUES(value), expiration = VALUES(expiration);"
            ;;
        3) # forget
            sql "DELETE FROM cache WHERE \`key\` = '${key}';"
            ;;
    esac
    ((OPS[cache]++))
}

do_activity_log() {
    local events=("created" "updated" "deleted" "login" "logout" "viewed" "exported")
    local event=${events[$(rand_between 0 6)]}
    local subject_id=$(rand_between 1 100)
    local causer_id=$(rand_between 1 5)
    sql "INSERT INTO activity_log (log_name, description, subject_type, subject_id, causer_type, causer_id, properties, event, created_at, updated_at)
         VALUES ('default', '${event}', 'App\\\\Models\\\\Order', ${subject_id}, 'App\\\\Models\\\\User', ${causer_id},
                 '{\"ip\":\"10.0.0.$(rand_between 1 254)\",\"ts\":$(date +%s)}', '${event}', NOW(), NOW());"
    ((OPS[activity]++))
}

do_token_touch() {
    local tid=$(rand_between 1 3)
    sql "UPDATE personal_access_tokens SET last_used_at = NOW() WHERE id = ${tid};"
    ((OPS[token]++))
}

do_order_lifecycle() {
    local action=$(rand_between 1 5)
    case $action in
        1|2) # new order
            ((ORDER_SEQ++))
            local uid=$(rand_between 1 5)
            local amount=$(rand_between 10 500).$(rand_between 10 99)
            local tax=$(echo "scale=2; $amount * 0.09" | bc 2>/dev/null || echo "5.00")
            local total=$(echo "scale=2; $amount + $tax + 8.99" | bc 2>/dev/null || echo "$amount")
            sql "INSERT INTO orders (user_id, order_number, status, subtotal, tax, shipping, total, currency, shipping_address, created_at, updated_at)
                 VALUES (${uid}, 'ORD-LOAD-${ORDER_SEQ}', 'pending', ${amount}, ${tax}, 8.99, ${total}, 'USD',
                         '{\"street\":\"$(rand_between 100 999) Test St\",\"city\":\"Portland\",\"state\":\"OR\",\"zip\":\"97201\"}',
                         NOW(), NOW());"
            # Add 1-3 items
            local items=$(rand_between 1 3)
            for i in $(seq 1 $items); do
                local pid=$(rand_between 1 5)
                local qty=$(rand_between 1 3)
                local price=$(rand_between 5 150).$(rand_between 10 99)
                local item_total=$(echo "scale=2; $price * $qty" | bc 2>/dev/null || echo "$price")
                sql "INSERT INTO order_items (order_id, product_id, quantity, unit_price, total, created_at, updated_at)
                     VALUES ((SELECT MAX(id) FROM orders WHERE order_number='ORD-LOAD-${ORDER_SEQ}'), ${pid}, ${qty}, ${price}, ${item_total}, NOW(), NOW());"
            done
            ;;
        3) # process
            sql "UPDATE orders SET status='processing', paid_at=NOW(), updated_at=NOW()
                 WHERE status='pending' ORDER BY RAND() LIMIT 1;"
            ;;
        4) # ship
            sql "UPDATE orders SET status='shipped', shipped_at=NOW(), updated_at=NOW()
                 WHERE status='processing' ORDER BY RAND() LIMIT 1;"
            ;;
        5) # deliver
            sql "UPDATE orders SET status='delivered', delivered_at=NOW(), updated_at=NOW()
                 WHERE status='shipped' ORDER BY RAND() LIMIT 1;"
            ;;
    esac
    ((OPS[order]++))
}

do_user_registration() {
    ((USER_SEQ++))
    sql "INSERT INTO users (name, email, password, settings, created_at, updated_at)
         VALUES ('LoadUser ${USER_SEQ}', 'loaduser${USER_SEQ}@example.com', '\$2y\$12\$fakehash.load${USER_SEQ}',
                 '{\"locale\":\"en\",\"theme\":\"$([ $((RANDOM % 2)) -eq 0 ] && echo light || echo dark)\"}',
                 NOW(), NOW());"
    # Create personal team
    sql "INSERT INTO teams (user_id, name, personal_team, created_at, updated_at)
         VALUES ((SELECT MAX(id) FROM users), 'LoadUser ${USER_SEQ} Team', 1, NOW(), NOW());"
    ((OPS[user]++))
}

do_notification() {
    ((NOTIF_SEQ++))
    local uid=$(rand_between 1 5)
    local types=("OrderShipped" "OrderDelivered" "PaymentReceived" "WelcomeEmail" "PasswordChanged")
    local type=${types[$(rand_between 0 4)]}
    sql "INSERT INTO notifications (id, type, notifiable_type, notifiable_id, data, created_at, updated_at)
         VALUES (UUID(), 'App\\\\Notifications\\\\${type}', 'App\\\\Models\\\\User', ${uid},
                 '{\"msg\":\"Notification ${NOTIF_SEQ}\",\"ts\":$(date +%s)}', NOW(), NOW());"
    # Sometimes mark old ones as read
    if [ $((RANDOM % 3)) -eq 0 ]; then
        sql "UPDATE notifications SET read_at = NOW()
             WHERE notifiable_id = ${uid} AND read_at IS NULL
             ORDER BY created_at LIMIT 1;"
    fi
    ((OPS[notification]++))
}

do_job_dispatch() {
    local action=$(rand_between 1 4)
    case $action in
        1|2) # dispatch new job
            sql "INSERT INTO jobs (queue, payload, attempts, available_at, created_at)
                 VALUES ('$([ $((RANDOM % 2)) -eq 0 ] && echo default || echo emails)',
                         '{\"uuid\":\"$(cat /proc/sys/kernel/random/uuid 2>/dev/null || echo $(date +%s)-$RANDOM)\",\"displayName\":\"App\\\\\\\\Jobs\\\\\\\\ProcessData\"}',
                         0, UNIX_TIMESTAMP(), UNIX_TIMESTAMP());"
            ;;
        3) # process (reserve + delete)
            sql "UPDATE jobs SET reserved_at = UNIX_TIMESTAMP(), attempts = attempts + 1
                 WHERE reserved_at IS NULL ORDER BY id LIMIT 1;"
            sql "DELETE FROM jobs WHERE reserved_at IS NOT NULL AND attempts > 0 ORDER BY id LIMIT 1;"
            ;;
        4) # fail a job
            if [ $((RANDOM % 5)) -eq 0 ]; then
                sql "INSERT INTO failed_jobs (uuid, connection, queue, payload, exception, failed_at)
                     VALUES (UUID(), 'database', 'default',
                             '{\"job\":\"ProcessData\"}',
                             'RuntimeException: Something went wrong at line $(rand_between 1 500)',
                             NOW());"
            fi
            ;;
    esac
    ((OPS[job]++))
}

do_burst() {
    local burst_type=$(rand_between 1 3)
    log "BURST: type=${burst_type}"
    case $burst_type in
        1) # Product price update (bulk)
            sql "UPDATE products SET price = ROUND(price * (1 + (RAND() * 0.1 - 0.05)), 2), updated_at = NOW()
                 WHERE deleted_at IS NULL;"
            ;;
        2) # Session cleanup
            sql "DELETE FROM sessions WHERE last_activity < (UNIX_TIMESTAMP() - 7200);"
            ;;
        3) # Batch of notifications
            for i in $(seq 1 10); do
                do_notification
            done
            ;;
    esac
    ((OPS[burst]++))
}

# ─── Main loop ───────────────────────────────────────────────────────────────

log "Starting sustained load for $((DURATION_SECS / 60)) minutes"
log "Target: ~3,000 events/hour (~0.8/sec)"

LAST_REPORT=$START
NEXT_SESSION=$START
NEXT_CACHE=$START
NEXT_ACTIVITY=$START
NEXT_TOKEN=$START
NEXT_ORDER=$START
NEXT_USER=$START
NEXT_NOTIFICATION=$START
NEXT_JOB=$START
NEXT_BURST=$((START + $(rand_between 900 1800)))

while [ $(date +%s) -lt $END ]; do
    NOW=$(date +%s)

    # Session touch — most frequent
    if [ $NOW -ge $NEXT_SESSION ]; then
        do_session_touch
        NEXT_SESSION=$((NOW + $(rand_between 1 3)))
    fi

    # Cache operations
    if [ $NOW -ge $NEXT_CACHE ]; then
        do_cache_op
        NEXT_CACHE=$((NOW + $(rand_between 3 8)))
    fi

    # Activity log
    if [ $NOW -ge $NEXT_ACTIVITY ]; then
        do_activity_log
        NEXT_ACTIVITY=$((NOW + $(rand_between 5 15)))
    fi

    # Token touch
    if [ $NOW -ge $NEXT_TOKEN ]; then
        do_token_touch
        NEXT_TOKEN=$((NOW + $(rand_between 10 20)))
    fi

    # Order lifecycle
    if [ $NOW -ge $NEXT_ORDER ]; then
        do_order_lifecycle
        NEXT_ORDER=$((NOW + $(rand_between 20 60)))
    fi

    # User registration
    if [ $NOW -ge $NEXT_USER ]; then
        do_user_registration
        NEXT_USER=$((NOW + $(rand_between 60 180)))
    fi

    # Notifications
    if [ $NOW -ge $NEXT_NOTIFICATION ]; then
        do_notification
        NEXT_NOTIFICATION=$((NOW + $(rand_between 30 90)))
    fi

    # Job dispatch
    if [ $NOW -ge $NEXT_JOB ]; then
        do_job_dispatch
        NEXT_JOB=$((NOW + $(rand_between 10 30)))
    fi

    # Burst
    if [ $NOW -ge $NEXT_BURST ]; then
        do_burst
        NEXT_BURST=$((NOW + $(rand_between 900 1800)))
    fi

    # Periodic report (every 5 minutes)
    if [ $((NOW - LAST_REPORT)) -ge 300 ]; then
        report
        LAST_REPORT=$NOW
    fi

    # Small sleep to avoid busy-loop (most tick logic handles its own timing)
    sleep 0.5
done

log "Load generation complete"
report
