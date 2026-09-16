-- =============================================================
-- Benchmark Queries — PostgreSQL (OLTP)
-- Run with: psql -U postgres -d ecommerce -f bench_postgres.sql
-- Each query has EXPLAIN ANALYZE to capture the execution plan.
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- Q1: Daily revenue by product category (last 30 days)
--     Tests: multi-table JOIN, GROUP BY, date truncation, filter
-- ─────────────────────────────────────────────────────────────
EXPLAIN ANALYZE
SELECT
    c.name                               AS category,
    DATE_TRUNC('day', o.created_at)      AS day,
    COUNT(DISTINCT o.order_id)           AS total_orders,
    SUM(oi.quantity * oi.unit_price)     AS revenue,
    AVG(oi.quantity * oi.unit_price)     AS avg_item_revenue
FROM orders o
JOIN order_items oi  ON oi.order_id   = o.order_id
JOIN products p      ON p.product_id  = oi.product_id
JOIN categories c    ON c.category_id = p.category_id
WHERE o.status IN ('shipped', 'delivered')
  AND o.created_at >= NOW() - INTERVAL '30 days'
GROUP BY c.name, DATE_TRUNC('day', o.created_at)
ORDER BY day DESC, revenue DESC;


-- ─────────────────────────────────────────────────────────────
-- Q2: Customer lifetime value — top 50 spenders
--     Tests: aggregation over JOIN, HAVING, ORDER BY computed col
-- ─────────────────────────────────────────────────────────────
EXPLAIN ANALYZE
SELECT
    cu.customer_id,
    cu.first_name || ' ' || cu.last_name   AS full_name,
    cu.city,
    COUNT(DISTINCT o.order_id)             AS orders_placed,
    SUM(oi.quantity * oi.unit_price)       AS lifetime_value,
    ROUND(AVG(oi.quantity * oi.unit_price), 2) AS avg_item_value,
    MAX(o.created_at)                      AS last_order_at
FROM customers cu
JOIN orders o        ON o.customer_id = cu.customer_id
JOIN order_items oi  ON oi.order_id   = o.order_id
WHERE o.status <> 'cancelled'
GROUP BY cu.customer_id, cu.first_name, cu.last_name, cu.city
HAVING COUNT(DISTINCT o.order_id) >= 2
ORDER BY lifetime_value DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────
-- Q3: Order fulfillment rate and average ticket by city
--     Tests: conditional aggregation, window-free percentage
-- ─────────────────────────────────────────────────────────────
EXPLAIN ANALYZE
SELECT
    city,
    COUNT(*)                                                       AS total_orders,
    COUNT(*) FILTER (WHERE status = 'delivered')                   AS delivered,
    COUNT(*) FILTER (WHERE status = 'cancelled')                   AS cancelled,
    ROUND(
        100.0
        * COUNT(*) FILTER (WHERE status = 'delivered')
        / NULLIF(COUNT(*), 0),
    2)                                                             AS fulfillment_pct,
    ROUND(AVG(total_amount), 2)                                    AS avg_ticket
FROM orders
GROUP BY city
ORDER BY fulfillment_pct DESC NULLS LAST;


-- ─────────────────────────────────────────────────────────────
-- Q4: Product performance — revenue, units sold, sell-through
--     Tests: three-way JOIN, aggregation, ratio computation
-- ─────────────────────────────────────────────────────────────
EXPLAIN ANALYZE
SELECT
    p.product_id,
    p.name                                AS product_name,
    c.name                                AS category,
    p.price                               AS current_price,
    p.stock                               AS current_stock,
    COUNT(DISTINCT oi.order_id)           AS times_ordered,
    SUM(oi.quantity)                      AS units_sold,
    ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue,
    ROUND(AVG(oi.unit_price), 2)          AS avg_sell_price
FROM products p
JOIN categories c    ON c.category_id = p.category_id
JOIN order_items oi  ON oi.product_id  = p.product_id
JOIN orders o        ON o.order_id     = oi.order_id
WHERE o.status <> 'cancelled'
GROUP BY p.product_id, p.name, c.name, p.price, p.stock
ORDER BY total_revenue DESC
LIMIT 25;


-- ─────────────────────────────────────────────────────────────
-- Q5: Payment method breakdown with share percentage
--     Tests: window function (SUM OVER PARTITION), multi-level agg
-- ─────────────────────────────────────────────────────────────
EXPLAIN ANALYZE
SELECT
    method,
    status,
    COUNT(*)                                            AS tx_count,
    ROUND(SUM(amount), 2)                               AS total_amount,
    ROUND(AVG(amount), 2)                               AS avg_amount,
    ROUND(
        100.0 * COUNT(*)
        / SUM(COUNT(*)) OVER (PARTITION BY method),
    2)                                                  AS status_share_pct
FROM payments
GROUP BY method, status
ORDER BY method, total_amount DESC;
