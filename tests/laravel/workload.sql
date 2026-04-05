-- =============================================================================
-- Laravel-style DML workload
-- Simulates typical Eloquent ORM operations: creates, updates, soft deletes,
-- JSON column mutations, bulk inserts, and order lifecycle state machines.
-- =============================================================================

-- ─── Phase 0: Framework bookkeeping (artisan migrate writes these) ──────────

INSERT INTO laravel_app.migrations (migration, batch) VALUES
    ('2014_10_12_000000_create_users_table', 1),
    ('2014_10_12_100000_create_password_reset_tokens_table', 1),
    ('2019_12_14_000001_create_personal_access_tokens_table', 1),
    ('2024_08_14_000000_create_sessions_table', 1),
    ('2024_08_14_000001_create_cache_table', 1),
    ('2024_08_14_000002_create_jobs_table', 1),
    ('2024_09_01_000000_create_teams_table', 2),
    ('2024_09_01_000001_create_products_table', 2),
    ('2024_09_01_000002_create_orders_table', 2),
    ('2024_09_01_000003_create_notifications_table', 2),
    ('2024_09_01_000004_create_activity_log_table', 2);

-- ─── Phase 1: User registration (Eloquent Model::create) ───────────────────

INSERT INTO laravel_app.users (name, email, password, settings, created_at, updated_at)
VALUES
    ('Alice Johnson',   'alice@example.com',   '$2y$12$fakehash.alice',   '{"locale":"en","theme":"light","notifications":{"email":true,"sms":false}}', NOW(), NOW()),
    ('Bob Smith',       'bob@example.com',     '$2y$12$fakehash.bob',     '{"locale":"en","theme":"dark","notifications":{"email":true,"sms":true}}',   NOW(), NOW()),
    ('Carlos Rivera',   'carlos@example.com',  '$2y$12$fakehash.carlos',  '{"locale":"es","theme":"light"}',                                           NOW(), NOW()),
    ('Diana Chen',      'diana@example.com',   '$2y$12$fakehash.diana',   NULL,                                                                        NOW(), NOW()),
    ('Eve Kowalski',    'eve@example.com',     '$2y$12$fakehash.eve',     '{"locale":"pl","theme":"dark","beta_features":true}',                        NOW(), NOW());

-- ─── Phase 2: Teams (belongsTo / hasMany) ──────────────────────────────────

INSERT INTO laravel_app.teams (user_id, name, personal_team, created_at, updated_at)
VALUES
    (1, 'Alice''s Team', 1, NOW(), NOW()),
    (2, 'Bob''s Team',   1, NOW(), NOW()),
    (1, 'Engineering',   0, NOW(), NOW());

-- Assign Alice to Engineering team
UPDATE laravel_app.users SET current_team_id = 3, updated_at = NOW() WHERE id = 1;

-- ─── Phase 3: Product catalog (bulk insert, JSON arrays) ───────────────────

INSERT INTO laravel_app.products (sku, name, description, price, compare_at_price, cost, quantity, is_active, weight_grams, metadata, tags, published_at, created_at, updated_at)
VALUES
    ('WIDGET-001', 'Premium Widget',     'A high-quality widget for all your needs',   29.99, 39.99, 12.50, 150, 1, 250,  '{"material":"aluminum","dimensions":{"l":10,"w":5,"h":3}}', '["featured","sale"]',    NOW(), NOW(), NOW()),
    ('WIDGET-002', 'Basic Widget',       'An affordable everyday widget',              9.99,  NULL,  4.00,  500, 1, 120,  '{"material":"plastic","dimensions":{"l":8,"w":4,"h":2}}',   '["bestseller"]',         NOW(), NOW(), NOW()),
    ('GADGET-001', 'Super Gadget',       'The ultimate gadget experience',             149.99, 199.99, 60.00, 25, 1, 800, '{"material":"carbon fiber","warranty_months":24}',           '["new","premium"]',      NOW(), NOW(), NOW()),
    ('CABLE-USB',  'USB-C Cable 2m',     NULL,                                         7.99,  NULL,  1.20,  1000, 1, 45,  NULL,                                                       NULL,                     NOW(), NOW(), NOW()),
    ('CASE-PHONE', 'Phone Case - Clear', 'Crystal clear phone protection',             14.99, NULL,  3.00,  300, 1, 30,  '{"compatible_models":["iPhone 15","iPhone 15 Pro"]}',        '["accessories"]',        NULL, NOW(), NOW());

-- ─── Phase 4: Place orders (state machine: pending → processing → shipped) ─

-- Alice places an order
INSERT INTO laravel_app.orders (user_id, order_number, status, subtotal, tax, shipping, total, currency, shipping_address, billing_address, created_at, updated_at)
VALUES (1, 'ORD-2026-00001', 'pending', 59.97, 5.40, 8.99, 74.36, 'USD',
    '{"name":"Alice Johnson","street":"123 Main St","city":"Portland","state":"OR","zip":"97201","country":"US"}',
    '{"name":"Alice Johnson","street":"123 Main St","city":"Portland","state":"OR","zip":"97201","country":"US"}',
    NOW(), NOW());

INSERT INTO laravel_app.order_items (order_id, product_id, quantity, unit_price, total, options, created_at, updated_at)
VALUES
    (1, 1, 2, 29.99, 59.98, '{"gift_wrap":true}', NOW(), NOW()),
    (1, 4, 1, 7.99,  7.99,  NULL,                 NOW(), NOW());

-- Update stock (Eloquent decrement)
UPDATE laravel_app.products SET quantity = quantity - 2, updated_at = NOW() WHERE id = 1;
UPDATE laravel_app.products SET quantity = quantity - 1, updated_at = NOW() WHERE id = 4;

-- Bob places an order
INSERT INTO laravel_app.orders (user_id, order_number, status, subtotal, tax, shipping, total, currency, notes, shipping_address, created_at, updated_at)
VALUES (2, 'ORD-2026-00002', 'pending', 149.99, 13.50, 0.00, 163.49, 'USD', 'Please leave at door',
    '{"name":"Bob Smith","street":"456 Oak Ave","city":"Seattle","state":"WA","zip":"98101","country":"US"}',
    NOW(), NOW());

INSERT INTO laravel_app.order_items (order_id, product_id, quantity, unit_price, total, options, created_at, updated_at)
VALUES (2, 3, 1, 149.99, 149.99, '{"color":"midnight black"}', NOW(), NOW());

UPDATE laravel_app.products SET quantity = quantity - 1, updated_at = NOW() WHERE id = 3;

-- ─── Phase 5: Order lifecycle updates (Eloquent save + touch) ──────────────

-- Process Alice's order
UPDATE laravel_app.orders SET status = 'processing', updated_at = NOW() WHERE order_number = 'ORD-2026-00001';

-- Ship Alice's order
UPDATE laravel_app.orders SET status = 'shipped', shipped_at = NOW(), updated_at = NOW() WHERE order_number = 'ORD-2026-00001';

-- Process and ship Bob's order
UPDATE laravel_app.orders SET status = 'processing', paid_at = NOW(), updated_at = NOW() WHERE order_number = 'ORD-2026-00002';
UPDATE laravel_app.orders SET status = 'shipped', shipped_at = NOW(), updated_at = NOW() WHERE order_number = 'ORD-2026-00002';

-- Deliver Alice's order
UPDATE laravel_app.orders SET status = 'delivered', delivered_at = NOW(), updated_at = NOW() WHERE order_number = 'ORD-2026-00001';

-- ─── Phase 6: User profile updates (JSON column mutations) ─────────────────

-- Eloquent: $user->settings = array_merge($user->settings, [...])
UPDATE laravel_app.users SET
    settings = JSON_SET(settings, '$.theme', 'dark', '$.two_factor_enabled', true),
    updated_at = NOW()
WHERE id = 1;

-- Verify email
UPDATE laravel_app.users SET email_verified_at = NOW(), updated_at = NOW() WHERE id = 1;
UPDATE laravel_app.users SET email_verified_at = NOW(), updated_at = NOW() WHERE id = 2;

-- User uploads profile photo
UPDATE laravel_app.users SET profile_photo_path = 'profile-photos/alice-abc123.jpg', updated_at = NOW() WHERE id = 1;

-- ─── Phase 7: Notifications (UUID PK, polymorphic, JSON data) ──────────────

INSERT INTO laravel_app.notifications (id, type, notifiable_type, notifiable_id, data, created_at, updated_at)
VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'App\\Notifications\\OrderShipped', 'App\\Models\\User', 1,
     '{"order_id":1,"order_number":"ORD-2026-00001","tracking_number":"1Z999AA10123456784","carrier":"UPS"}',
     NOW(), NOW()),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', 'App\\Notifications\\OrderShipped', 'App\\Models\\User', 2,
     '{"order_id":2,"order_number":"ORD-2026-00002","tracking_number":"9405511899223100001234","carrier":"USPS"}',
     NOW(), NOW()),
    ('c3d4e5f6-a7b8-9012-cdef-123456789012', 'App\\Notifications\\WelcomeEmail', 'App\\Models\\User', 5,
     '{"user_name":"Eve Kowalski"}',
     NOW(), NOW());

-- Mark notification as read
UPDATE laravel_app.notifications SET read_at = NOW() WHERE id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';

-- ─── Phase 7b: Framework tables (sessions, cache, jobs, tokens) ────────────

-- Sessions — SessionManager writes on every request (database driver)
INSERT INTO laravel_app.sessions (id, user_id, ip_address, user_agent, payload, last_activity)
VALUES
    ('abc123def456abc123def456abc123def456abc1', 1, '192.168.1.100', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiYWJjMTIzZGVmNDU2...base64payload', UNIX_TIMESTAMP()),
    ('def456abc123def456abc123def456abc123def4', 2, '10.0.0.42',     'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',        'YTozOntzOjY6Il90b2tlbiI7czo0MDoiZGVmNDU2YWJjMTIz...base64payload', UNIX_TIMESTAMP());

-- Session touch (happens every request — Eloquent updates last_activity)
UPDATE laravel_app.sessions SET last_activity = UNIX_TIMESTAMP(), payload = 'YTozOntzOjY6Il90b2...updated_payload' WHERE id = 'abc123def456abc123def456abc123def456abc1';

-- Cache — Cache::put('key', 'value', $ttl)
INSERT INTO laravel_app.cache (`key`, value, expiration) VALUES
    ('laravel_cache_user_1_permissions', 's:45:"a:3:{i:0;s:10:\\"view-posts\\";i:1;s:12:\\"create-posts\\";}"', UNIX_TIMESTAMP() + 3600),
    ('laravel_cache_settings',          's:128:"a:5:{s:9:\\"app_name\\";s:10:\\"MyLaravel\\";}"',               UNIX_TIMESTAMP() + 86400),
    ('laravel_cache_rate_limit:api:1',  'i:47;',                                                                 UNIX_TIMESTAMP() + 60);

-- Cache::forget
DELETE FROM laravel_app.cache WHERE `key` = 'laravel_cache_rate_limit:api:1';

-- Cache locks (atomic locks for queue workers)
INSERT INTO laravel_app.cache_locks (`key`, owner, expiration) VALUES
    ('laravel_lock_order_process_1', 'abc123owner', UNIX_TIMESTAMP() + 10);
DELETE FROM laravel_app.cache_locks WHERE `key` = 'laravel_lock_order_process_1';

-- Jobs — dispatch(new ProcessOrderJob($order))
INSERT INTO laravel_app.jobs (queue, payload, attempts, reserved_at, available_at, created_at) VALUES
    ('default', '{"uuid":"d4e5f6a7-b8c9-0123-def0-123456789abc","displayName":"App\\\\Jobs\\\\ProcessOrderPayment","job":"Illuminate\\\\Queue\\\\CallQueuedHandler@call","data":{"commandName":"App\\\\Jobs\\\\ProcessOrderPayment","command":"O:35:\\"App\\\\Jobs\\\\ProcessOrderPayment\\":1:{s:7:\\"orderId\\";i:1;}"}}', 0, NULL, UNIX_TIMESTAMP(), UNIX_TIMESTAMP()),
    ('emails', '{"uuid":"e5f6a7b8-c9d0-1234-ef01-23456789abcd","displayName":"App\\\\Jobs\\\\SendOrderConfirmation","job":"Illuminate\\\\Queue\\\\CallQueuedHandler@call","data":{"commandName":"App\\\\Jobs\\\\SendOrderConfirmation","command":"O:39:\\"App\\\\Jobs\\\\SendOrderConfirmation\\":1:{s:7:\\"orderId\\";i:1;}"}}', 0, NULL, UNIX_TIMESTAMP(), UNIX_TIMESTAMP());

-- Worker picks up and processes a job (reserve then delete)
UPDATE laravel_app.jobs SET reserved_at = UNIX_TIMESTAMP(), attempts = 1 WHERE id = 1;
DELETE FROM laravel_app.jobs WHERE id = 1;

-- Job batches — Bus::batch([...])
INSERT INTO laravel_app.job_batches (id, name, total_jobs, pending_jobs, failed_jobs, failed_job_ids, options, created_at) VALUES
    ('batch-uuid-001', 'Import Products', 50, 50, 0, '[]', NULL, UNIX_TIMESTAMP());

-- Progress on batch
UPDATE laravel_app.job_batches SET pending_jobs = 25 WHERE id = 'batch-uuid-001';
UPDATE laravel_app.job_batches SET pending_jobs = 0, finished_at = UNIX_TIMESTAMP() WHERE id = 'batch-uuid-001';

-- Failed job
INSERT INTO laravel_app.failed_jobs (uuid, connection, queue, payload, exception, failed_at) VALUES
    ('f6a7b8c9-d0e1-2345-f012-3456789abcde', 'database', 'emails',
     '{"uuid":"e5f6a7b8-c9d0-1234-ef01-23456789abcd","displayName":"App\\\\Jobs\\\\SendOrderConfirmation"}',
     'Symfony\\Component\\Mailer\\Exception\\TransportException: Connection to smtp.example.com timed out in /vendor/symfony/mailer/Transport/Smtp/SmtpTransport.php:234\nStack trace:\n#0 ...',
     NOW());

-- Sanctum tokens — $user->createToken('api-token')
INSERT INTO laravel_app.personal_access_tokens (tokenable_type, tokenable_id, name, token, abilities, last_used_at, created_at, updated_at) VALUES
    ('App\\Models\\User', 1, 'api-token',     '7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d', '["*"]',                      NULL,  NOW(), NOW()),
    ('App\\Models\\User', 1, 'mobile-token',  'a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2', '["read-orders","read-products"]', NULL,  NOW(), NOW()),
    ('App\\Models\\User', 2, 'ci-token',      '1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef', '["deploy"]',                  NOW(), NOW(), NOW());

-- Token used (Sanctum middleware touches last_used_at on every authenticated request)
UPDATE laravel_app.personal_access_tokens SET last_used_at = NOW() WHERE id = 1;
UPDATE laravel_app.personal_access_tokens SET last_used_at = NOW() WHERE id = 1;
UPDATE laravel_app.personal_access_tokens SET last_used_at = NOW() WHERE id = 1;

-- Revoke token — $token->delete()
DELETE FROM laravel_app.personal_access_tokens WHERE id = 2;

-- Password reset — Password::sendResetLink()
INSERT INTO laravel_app.password_reset_tokens (email, token, created_at) VALUES
    ('bob@example.com', '$2y$12$fakehashforpasswordreset.token', NOW());

-- Token consumed after password reset
DELETE FROM laravel_app.password_reset_tokens WHERE email = 'bob@example.com';

-- Session cleanup (artisan session:gc or gc_probability)
DELETE FROM laravel_app.sessions WHERE last_activity < (UNIX_TIMESTAMP() - 7200);

-- ─── Phase 8: Activity log (audit trail, high volume) ──────────────────────

INSERT INTO laravel_app.activity_log (log_name, description, subject_type, subject_id, causer_type, causer_id, properties, event, created_at, updated_at)
VALUES
    ('default', 'created',   'App\\Models\\Order', 1, 'App\\Models\\User', 1, '{"attributes":{"order_number":"ORD-2026-00001","status":"pending","total":"74.36"}}', 'created', NOW(), NOW()),
    ('default', 'updated',   'App\\Models\\Order', 1, 'App\\Models\\User', 1, '{"old":{"status":"pending"},"attributes":{"status":"processing"}}', 'updated', NOW(), NOW()),
    ('default', 'updated',   'App\\Models\\Order', 1, 'App\\Models\\User', 1, '{"old":{"status":"processing"},"attributes":{"status":"shipped"}}', 'updated', NOW(), NOW()),
    ('default', 'updated',   'App\\Models\\Order', 1, 'App\\Models\\User', 1, '{"old":{"status":"shipped"},"attributes":{"status":"delivered"}}', 'updated', NOW(), NOW()),
    ('default', 'created',   'App\\Models\\Order', 2, 'App\\Models\\User', 2, '{"attributes":{"order_number":"ORD-2026-00002","status":"pending","total":"163.49"}}', 'created', NOW(), NOW()),
    ('auth',    'logged_in', NULL,                 NULL, 'App\\Models\\User', 1, '{"ip":"192.168.1.100","user_agent":"Mozilla/5.0"}', 'login', NOW(), NOW()),
    ('auth',    'logged_in', NULL,                 NULL, 'App\\Models\\User', 2, '{"ip":"10.0.0.42","user_agent":"Mozilla/5.0"}',     'login', NOW(), NOW());

-- ─── Phase 9: Soft deletes and cancellation ────────────────────────────────

-- Eve deletes her account (soft delete)
UPDATE laravel_app.users SET deleted_at = NOW(), updated_at = NOW() WHERE id = 5;

-- Unpublish a product (soft delete)
UPDATE laravel_app.products SET deleted_at = NOW(), updated_at = NOW() WHERE sku = 'CASE-PHONE';

-- Cancel Bob's order (state + soft delete)
UPDATE laravel_app.orders SET status = 'cancelled', cancelled_at = NOW(), updated_at = NOW() WHERE order_number = 'ORD-2026-00002';

-- ─── Phase 10: Restore soft-deleted record ─────────────────────────────────

-- Eve changes her mind (Model::restore)
UPDATE laravel_app.users SET deleted_at = NULL, updated_at = NOW() WHERE id = 5;

-- ─── Phase 11: Bulk update (Eloquent query builder) ────────────────────────

-- Mark all unverified users' settings
UPDATE laravel_app.users
SET settings = JSON_SET(COALESCE(settings, '{}'), '$.needs_verification', true),
    updated_at = NOW()
WHERE email_verified_at IS NULL;

-- Price increase across product line
UPDATE laravel_app.products
SET price = ROUND(price * 1.05, 2),
    updated_at = NOW()
WHERE deleted_at IS NULL AND is_active = 1;

-- ─── Phase 12: Hard delete (rare in Laravel, but happens) ──────────────────

-- Clean up read notifications older than... (well, all of them for testing)
DELETE FROM laravel_app.notifications WHERE read_at IS NOT NULL;
