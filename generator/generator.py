#!/usr/bin/env python
"""
E-Commerce CDC Data Generator
------------------------------
Continuously executes INSERT, UPDATE, and DELETE operations against
PostgreSQL so that PeerDB captures every change and forwards it to
ClickHouse in real time.

Target: 300 000+ operations
Operation mix (approximate):
  35% new orders   (INSERT orders + order_items + payments)
  25% order status advances  (UPDATE orders)
  15% product stock / price  (UPDATE products)
  10% new customer registrations  (INSERT customers)
  10% payment status transitions  (UPDATE payments)
   5% cancelled-order cleanup     (DELETE orders cascade)
"""

import os
import time
import random
import logging
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

# ─── Configuration ────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

LOG_EVERY   = 1_000   # log a summary line every N operations
BATCH_SIZE  = 25      # ops committed per transaction
SLEEP_MS    = 30      # ms to sleep between batches (adjust for speed)

# ─── Constants ────────────────────────────────────────────────────────────────
ORDER_STATUSES  = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
PAY_METHODS     = ["credit_card", "debit_card", "paypal", "transfer"]
PAY_STATUSES    = ["pending", "completed", "failed", "refunded"]
CITIES          = [
    "Mérida", "Ciudad de México", "Guadalajara", "Monterrey",
    "Cancún", "Puebla", "Tijuana", "León", "Querétaro", "Hermosillo",
]

fake = Faker(["es_MX", "en_US"])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─── Database helpers ─────────────────────────────────────────────────────────
def connect() -> psycopg2.extensions.connection:
    """Retry connection until PostgreSQL is ready."""
    for attempt in range(20):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            log.info("Connected to PostgreSQL at %s:%s/%s",
                     DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Connection attempt %d/20 failed: %s", attempt + 1, exc)
            time.sleep(5)
    raise RuntimeError("Could not connect to PostgreSQL after 20 attempts.")


# ─── Seed helpers ─────────────────────────────────────────────────────────────
def seed_customers(cur, n: int = 600) -> None:
    rows = []
    for _ in range(n):
        rows.append((
            fake.unique.email(),
            fake.first_name(),
            fake.last_name(),
            fake.phone_number()[:20],
            random.choice(CITIES),
        ))
    execute_values(cur, """
        INSERT INTO customers (email, first_name, last_name, phone, city)
        VALUES %s
        ON CONFLICT (email) DO NOTHING
    """, rows)
    log.info("Seeded %d customers.", n)


def seed_products(cur, n: int = 300) -> None:
    cur.execute("SELECT category_id FROM categories")
    cat_ids = [r[0] for r in cur.fetchall()]
    rows = []
    for _ in range(n):
        rows.append((
            random.choice(cat_ids),
            fake.catch_phrase()[:200],
            f"SKU-{fake.unique.bothify('??##??##??')}",
            round(random.uniform(9.99, 2499.99), 2),
            random.randint(0, 1000),
        ))
    execute_values(cur, """
        INSERT INTO products (category_id, name, sku, price, stock)
        VALUES %s
        ON CONFLICT (sku) DO NOTHING
    """, rows)
    log.info("Seeded %d products.", n)


# ─── Operation functions ──────────────────────────────────────────────────────
def op_new_order(cur) -> None:
    """INSERT: new order with 1-5 line items and a payment record."""
    cur.execute(
        "SELECT customer_id, city FROM customers ORDER BY RANDOM() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return
    customer_id, city = row

    cur.execute("""
        SELECT product_id, price
        FROM products
        WHERE is_active = TRUE AND stock > 0
        ORDER BY RANDOM()
        LIMIT %s
    """, (random.randint(1, 5),))
    items = cur.fetchall()
    if not items:
        return

    # Compute total
    item_rows = []
    total = Decimal("0")
    for prod_id, price in items:
        qty   = random.randint(1, 4)
        price = Decimal(str(price))
        total += price * qty
        item_rows.append((prod_id, qty, float(price)))

    # Insert order header
    cur.execute("""
        INSERT INTO orders (customer_id, status, total_amount, city)
        VALUES (%s, 'pending', %s, %s)
        RETURNING order_id
    """, (customer_id, float(total), city))
    order_id = cur.fetchone()[0]

    # Insert line items
    execute_values(cur, """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES %s
    """, [(order_id, pid, qty, up) for pid, qty, up in item_rows])

    # Insert payment
    pay_status = random.choices(
        ["pending", "completed", "failed"],
        weights=[20, 70, 10]
    )[0]
    cur.execute("""
        INSERT INTO payments (order_id, amount, method, status, transaction_ref)
        VALUES (%s, %s, %s, %s, %s)
    """, (order_id, float(total), random.choice(PAY_METHODS),
          pay_status, fake.uuid4()))


def op_advance_order_status(cur) -> None:
    """UPDATE: advance up to 15 orders along the status lifecycle."""
    cur.execute("""
        SELECT order_id, status
        FROM orders
        WHERE status NOT IN ('delivered', 'cancelled')
        ORDER BY RANDOM()
        LIMIT 15
    """)
    for order_id, status in cur.fetchall():
        idx         = ORDER_STATUSES.index(status)
        next_status = ORDER_STATUSES[min(idx + 1, len(ORDER_STATUSES) - 1)]
        cur.execute("""
            UPDATE orders
            SET status = %s, updated_at = NOW()
            WHERE order_id = %s
        """, (next_status, order_id))


def op_update_product(cur) -> None:
    """UPDATE: random price/stock changes on up to 20 products."""
    cur.execute(
        "SELECT product_id FROM products ORDER BY RANDOM() LIMIT 20"
    )
    for (pid,) in cur.fetchall():
        delta     = random.randint(-15, 60)
        new_price = round(random.uniform(9.99, 2499.99), 2)
        cur.execute("""
            UPDATE products
            SET stock      = GREATEST(stock + %s, 0),
                price      = %s,
                updated_at = NOW()
            WHERE product_id = %s
        """, (delta, new_price, pid))


def op_new_customer(cur) -> None:
    """INSERT: register a fresh customer."""
    try:
        cur.execute("""
            INSERT INTO customers (email, first_name, last_name, phone, city)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            fake.unique.email(),
            fake.first_name(),
            fake.last_name(),
            fake.phone_number()[:20],
            random.choice(CITIES),
        ))
    except psycopg2.errors.UniqueViolation:
        pass  # duplicate email — just skip


def op_update_payment(cur) -> None:
    """UPDATE: transition old pending payments to completed or failed."""
    cur.execute("""
        UPDATE payments
        SET status       = CASE WHEN RANDOM() > 0.12 THEN 'completed' ELSE 'failed' END,
            processed_at = NOW()
        WHERE status    = 'pending'
          AND processed_at < NOW() - INTERVAL '45 seconds'
    """)


def op_cleanup_cancelled(cur) -> None:
    """DELETE: remove old cancelled orders (cascades to order_items)."""
    cur.execute("""
        DELETE FROM orders
        WHERE status    = 'cancelled'
          AND updated_at < NOW() - INTERVAL '3 minutes'
    """)


# ─── Main loop ────────────────────────────────────────────────────────────────
OP_TABLE = [
    (op_new_order,            35),
    (op_advance_order_status, 25),
    (op_update_product,       15),
    (op_new_customer,         10),
    (op_update_payment,       10),
    (op_cleanup_cancelled,     5),
]
FUNCS, WEIGHTS = zip(*OP_TABLE)


def main() -> None:
    conn = connect()
    cur  = conn.cursor()

    # ── Initial seed ──────────────────────────────────────────
    log.info("Running initial seed …")
    seed_customers(cur, 600)
    seed_products(cur, 300)
    conn.commit()
    log.info("Seed complete. Starting continuous generation …")

    total_ops = 0
    t_start   = time.time()

    try:
        while True:
            try:
                chosen = random.choices(FUNCS, weights=WEIGHTS, k=BATCH_SIZE)
                for fn in chosen:
                    fn(cur)
                    total_ops += 1

                conn.commit()

                if total_ops % LOG_EVERY == 0:
                    elapsed = time.time() - t_start
                    rate    = total_ops / elapsed if elapsed else 0
                    log.info(
                        "ops=%s  rate=%.1f ops/s  elapsed=%.0fs",
                        f"{total_ops:,}", rate, elapsed
                    )

                time.sleep(SLEEP_MS / 1000)

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                log.error("Connection lost (%s). Reconnecting …", exc)
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect()
                cur  = conn.cursor()

            except Exception as exc:
                log.error("Unexpected error: %s", exc)
                conn.rollback()

    except KeyboardInterrupt:
        log.info("Generator stopped. Total operations: %s", f"{total_ops:,}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
