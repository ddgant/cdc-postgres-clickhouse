-- =============================================================
-- ClickHouse OLAP Target Tables
-- Engine: ReplacingMergeTree — the cdc_worker writes every change
-- as a new row with an increasing _version. Query with FINAL (or
-- filter _is_deleted = 0) to see only the latest state per row.
-- =============================================================

CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.categories
(
    category_id  Int32,
    name         String,
    description  String,
    created_at   DateTime64(6, 'UTC'),
    _version     Int64,
    _is_deleted  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PRIMARY KEY category_id
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS ecommerce.customers
(
    customer_id  Int32,
    email        String,
    first_name   String,
    last_name    String,
    phone        String,
    city         String,
    country      String,
    created_at   DateTime64(6, 'UTC'),
    updated_at   DateTime64(6, 'UTC'),
    _version     Int64,
    _is_deleted  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PRIMARY KEY customer_id
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS ecommerce.products
(
    product_id   Int32,
    category_id  Int32,
    name         String,
    sku          String,
    price        Decimal(10, 2),
    stock        Int32,
    is_active    UInt8,
    created_at   DateTime64(6, 'UTC'),
    updated_at   DateTime64(6, 'UTC'),
    _version     Int64,
    _is_deleted  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PRIMARY KEY product_id
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS ecommerce.orders
(
    order_id      Int32,
    customer_id   Int32,
    status        String,
    total_amount  Decimal(10, 2),
    city          String,
    created_at    DateTime64(6, 'UTC'),
    updated_at    DateTime64(6, 'UTC'),
    _version      Int64,
    _is_deleted   UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PRIMARY KEY order_id
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS ecommerce.order_items
(
    item_id      Int32,
    order_id     Int32,
    product_id   Int32,
    quantity     Int32,
    unit_price   Decimal(10, 2),
    _version     Int64,
    _is_deleted  UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PRIMARY KEY item_id
ORDER BY item_id;

CREATE TABLE IF NOT EXISTS ecommerce.payments
(
    payment_id      Int32,
    order_id        Int32,
    amount          Decimal(10, 2),
    method          String,
    status          String,
    transaction_ref String,
    processed_at    DateTime64(6, 'UTC'),
    _version        Int64,
    _is_deleted     UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(_version)
PRIMARY KEY payment_id
ORDER BY payment_id;
