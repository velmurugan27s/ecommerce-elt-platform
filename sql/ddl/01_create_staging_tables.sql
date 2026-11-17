BEGIN;

-- =========================================================
-- 1. CUSTOMERS
-- Grain: one row per customer_id per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id VARCHAR(32) NOT NULL,
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix VARCHAR(5),
    customer_city TEXT,
    customer_state VARCHAR(2),
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (customer_id, _batch_id)
);

-- =========================================================
-- 2. GEOLOCATION
-- Grain: one row per distinct source record per batch
-- No unique postal-code key exists.
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_geolocation (
    geolocation_zip_code_prefix VARCHAR(5),
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
    geolocation_city TEXT,
    geolocation_state VARCHAR(2),
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP
);

-- =========================================================
-- 3. ORDER ITEMS
-- Grain: one row per order item per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_id VARCHAR(32) NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(32),
    seller_id VARCHAR(32),
    shipping_limit_date TIMESTAMP,
    price NUMERIC(12,2),
    freight_value NUMERIC(12,2),
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id, _batch_id)
);

-- =========================================================
-- 4. PAYMENTS
-- Grain: one row per payment sequence per order per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_payments (
    order_id VARCHAR(32) NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value NUMERIC(12,2),
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (order_id, payment_sequential, _batch_id)
);

-- =========================================================
-- 5. REVIEWS
-- Grain: one row per review_id + order_id per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_reviews (
    review_id VARCHAR(32) NOT NULL,
    order_id VARCHAR(32) NOT NULL,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (review_id, order_id, _batch_id)
);

-- =========================================================
-- 6. ORDERS
-- Grain: one row per order per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id VARCHAR(32) NOT NULL,
    customer_id VARCHAR(32),
    order_status VARCHAR(30),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (order_id, _batch_id)
);

-- =========================================================
-- 7. PRODUCTS
-- Grain: one row per product per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id VARCHAR(32) NOT NULL,
    product_category_name TEXT,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER,
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (product_id, _batch_id)
);

-- =========================================================
-- 8. SELLERS
-- Grain: one row per seller per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_sellers (
    seller_id VARCHAR(32) NOT NULL,
    seller_zip_code_prefix VARCHAR(5),
    seller_city TEXT,
    seller_state VARCHAR(2),
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (seller_id, _batch_id)
);

-- =========================================================
-- 9. CATEGORY TRANSLATION
-- Grain: one row per Portuguese category per batch
-- =========================================================

CREATE TABLE IF NOT EXISTS staging.stg_category_translation (
    product_category_name TEXT NOT NULL,
    product_category_name_english TEXT,
    _batch_id VARCHAR(64) NOT NULL,
    _processed_at_utc TIMESTAMP,
    PRIMARY KEY (product_category_name, _batch_id)
);

-- =========================================================
-- INDEXES FOR COMMON JOIN AND FILTER COLUMNS
-- =========================================================

CREATE INDEX IF NOT EXISTS idx_stg_orders_customer
    ON staging.stg_orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_product
    ON staging.stg_order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_seller
    ON staging.stg_order_items (seller_id);

CREATE INDEX IF NOT EXISTS idx_stg_payments_order
    ON staging.stg_payments (order_id);

CREATE INDEX IF NOT EXISTS idx_stg_reviews_order
    ON staging.stg_reviews (order_id);

CREATE INDEX IF NOT EXISTS idx_stg_geolocation_batch_zip
    ON staging.stg_geolocation (_batch_id, geolocation_zip_code_prefix);

COMMIT;
