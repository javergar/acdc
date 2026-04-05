-- =============================================================================
-- Laravel-style DML workload
-- Simulates typical Eloquent ORM operations: creates, updates, soft deletes,
-- JSON column mutations, bulk inserts, and order lifecycle state machines.
-- =============================================================================

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
