-- =============================================================
-- Benchmark Queries — ClickHouse (OLAP)
-- Run via: docker exec -i olap_clickhouse clickhouse-client --password clickhouse --multiquery < bench_clickhouse.sql
--
-- Key differences from PostgreSQL version:
--   • FINAL keyword: forces ReplacingMergeTree deduplication at query time
--   • _is_deleted = 0: excludes CDC-deleted rows (tombstones)
--   • toStartOfDay() instead of DATE_TRUNC
--   • countIf / sumIf instead of COUNT(*) FILTER (WHERE ...)
--   • concat() instead of ||
--   • Use EXPLAIN (no ANALYZE) for the query plan
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- Q1: Daily revenue by product category (last 30 days)
-- ─────────────────────────────────────────────────────────────
EXPLAIN
SELECT
    c.name                                   AS category,
    toStartOfDay(o.created_at)               AS day,
    uniq(o.order_id)                         AS total_orders,
    sum(oi.quantity * oi.unit_price)         AS revenue,
    avg(oi.quantity * oi.unit_price)         AS avg_item_revenue
FROM ecommerce.orders AS o FINAL
JOIN ecommerce.order_items AS oi FINAL  ON oi.order_id   = o.order_id
JOIN ecommerce.products AS p FINAL      ON p.product_id  = oi.product_id
JOIN ecommerce.categories AS c FINAL    ON c.category_id = p.category_id
WHERE o.status IN ('shipped', 'delivered')
  AND o._is_deleted  = 0
  AND oi._is_deleted = 0
  AND p._is_deleted  = 0
  AND o.created_at >= now() - INTERVAL 30 DAY
GROUP BY c.name, toStartOfDay(o.created_at)
ORDER BY day DESC, revenue DESC;


-- ─────────────────────────────────────────────────────────────
-- Q2: Customer lifetime value — top 50 spenders
-- ─────────────────────────────────────────────────────────────
EXPLAIN
SELECT
    cu.customer_id,
    concat(cu.first_name, ' ', cu.last_name)   AS full_name,
    cu.city,
    uniq(o.order_id)                           AS orders_placed,
    sum(oi.quantity * oi.unit_price)           AS lifetime_value,
    round(avg(oi.quantity * oi.unit_price), 2) AS avg_item_value,
    max(o.created_at)                          AS last_order_at
FROM ecommerce.customers AS cu FINAL
JOIN ecommerce.orders AS o FINAL        ON o.customer_id = cu.customer_id
JOIN ecommerce.order_items AS oi FINAL  ON oi.order_id   = o.order_id
WHERE o.status != 'cancelled'
  AND o._is_deleted  = 0
  AND cu._is_deleted = 0
  AND oi._is_deleted = 0
GROUP BY cu.customer_id, cu.first_name, cu.last_name, cu.city
HAVING uniq(o.order_id) >= 2
ORDER BY lifetime_value DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────
-- Q3: Order fulfillment rate and average ticket by city
-- ─────────────────────────────────────────────────────────────
EXPLAIN
SELECT
    city,
    count()                                        AS total_orders,
    countIf(status = 'delivered')                  AS delivered,
    countIf(status = 'cancelled')                  AS cancelled,
    round(
        100.0
        * countIf(status = 'delivered')
        / nullIf(count(), 0),
    2)                                             AS fulfillment_pct,
    round(avg(total_amount), 2)                    AS avg_ticket
FROM ecommerce.orders FINAL
WHERE _is_deleted = 0
GROUP BY city
ORDER BY fulfillment_pct DESC;


-- ─────────────────────────────────────────────────────────────
-- Q4: Product performance — revenue, units sold
-- ─────────────────────────────────────────────────────────────
EXPLAIN
SELECT
    p.product_id,
    p.name                                     AS product_name,
    c.name                                     AS category,
    p.price                                    AS current_price,
    p.stock                                    AS current_stock,
    uniq(oi.order_id)                          AS times_ordered,
    sum(oi.quantity)                           AS units_sold,
    round(sum(oi.quantity * oi.unit_price), 2) AS total_revenue,
    round(avg(oi.unit_price), 2)               AS avg_sell_price
FROM ecommerce.products AS p FINAL
JOIN ecommerce.categories AS c FINAL    ON c.category_id = p.category_id
JOIN ecommerce.order_items AS oi FINAL  ON oi.product_id = p.product_id
JOIN ecommerce.orders AS o FINAL        ON o.order_id    = oi.order_id
WHERE o.status != 'cancelled'
  AND o._is_deleted  = 0
  AND p._is_deleted  = 0
  AND oi._is_deleted = 0
GROUP BY p.product_id, p.name, c.name, p.price, p.stock
ORDER BY total_revenue DESC
LIMIT 25;


-- ─────────────────────────────────────────────────────────────
-- Q5: Payment method breakdown with share percentage
-- ─────────────────────────────────────────────────────────────
EXPLAIN
SELECT
    method,
    status,
    count()                AS tx_count,
    round(sum(amount), 2)  AS total_amount,
    round(avg(amount), 2)  AS avg_amount,
    round(
        100.0 * count()
        / sum(count()) OVER (PARTITION BY method),
    2)                     AS status_share_pct
FROM ecommerce.payments FINAL
WHERE _is_deleted = 0
GROUP BY method, status
ORDER BY method, total_amount DESC;
