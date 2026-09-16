-- =============================================================
-- E-Commerce Schema — 3rd Normal Form (3NF)
-- PostgreSQL OLTP Source for CDC (via wal2json logical decoding)
-- =============================================================

-- =============================================================
-- TABLE: categories
-- =============================================================
CREATE TABLE categories (
    category_id  SERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL UNIQUE,
    description  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- TABLE: customers
-- =============================================================
CREATE TABLE customers (
    customer_id  SERIAL PRIMARY KEY,
    email        VARCHAR(255) NOT NULL UNIQUE,
    first_name   VARCHAR(100) NOT NULL,
    last_name    VARCHAR(100) NOT NULL,
    phone        VARCHAR(20),
    city         VARCHAR(100),
    country      VARCHAR(100) DEFAULT 'Mexico',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- TABLE: products (belongs to one category — 3NF)
-- =============================================================
CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    category_id  INT NOT NULL REFERENCES categories(category_id),
    name         VARCHAR(255) NOT NULL,
    sku          VARCHAR(100) NOT NULL UNIQUE,
    price        NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    stock        INT NOT NULL DEFAULT 0 CHECK (stock >= 0),
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- TABLE: orders
-- =============================================================
CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL REFERENCES customers(customer_id),
    status       VARCHAR(30) NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled')),
    total_amount NUMERIC(10,2),
    city         VARCHAR(100),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- TABLE: order_items (resolves orders <-> products M:N)
-- =============================================================
CREATE TABLE order_items (
    item_id      SERIAL PRIMARY KEY,
    order_id     INT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id   INT NOT NULL REFERENCES products(product_id),
    quantity     INT NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0)
);

-- =============================================================
-- TABLE: payments
-- =============================================================
CREATE TABLE payments (
    payment_id      SERIAL PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES orders(order_id),
    amount          NUMERIC(10,2) NOT NULL,
    method          VARCHAR(30) NOT NULL
                    CHECK (method IN ('credit_card','debit_card','paypal','transfer')),
    status          VARCHAR(30) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','completed','failed','refunded')),
    transaction_ref VARCHAR(255) UNIQUE,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- Indexes
-- =============================================================
CREATE INDEX idx_products_category  ON products(category_id);
CREATE INDEX idx_products_active    ON products(is_active);
CREATE INDEX idx_orders_customer    ON orders(customer_id);
CREATE INDEX idx_orders_status      ON orders(status);
CREATE INDEX idx_orders_created     ON orders(created_at);
CREATE INDEX idx_order_items_order  ON order_items(order_id);
CREATE INDEX idx_order_items_prod   ON order_items(product_id);
CREATE INDEX idx_payments_order     ON payments(order_id);
CREATE INDEX idx_payments_status    ON payments(status);

-- =============================================================
-- Seed: Categories
-- =============================================================
INSERT INTO categories (name, description) VALUES
    ('Electronics',   'Smartphones, laptops, accessories'),
    ('Clothing',      'Men, women and kids apparel'),
    ('Home & Garden', 'Furniture, tools and garden supplies'),
    ('Sports',        'Fitness equipment and outdoor gear'),
    ('Books',         'Fiction, non-fiction and textbooks'),
    ('Food & Drink',  'Groceries, snacks and beverages'),
    ('Toys',          'Games and toys for all ages'),
    ('Beauty',        'Skincare, cosmetics and personal care');

-- =============================================================
-- CDC requirement: REPLICA IDENTITY FULL
-- Without this, UPDATE/DELETE events on the WAL only include the
-- primary key of the OLD row. With FULL, they include every
-- column of the old row, which the CDC worker needs to build a
-- complete "tombstone" row when a DELETE happens.
-- =============================================================
ALTER TABLE categories   REPLICA IDENTITY FULL;
ALTER TABLE customers    REPLICA IDENTITY FULL;
ALTER TABLE products     REPLICA IDENTITY FULL;
ALTER TABLE orders       REPLICA IDENTITY FULL;
ALTER TABLE order_items  REPLICA IDENTITY FULL;
ALTER TABLE payments     REPLICA IDENTITY FULL;
