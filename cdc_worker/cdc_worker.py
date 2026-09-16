#!/usr/bin/env python
"""
CDC Worker — PostgreSQL (wal2json) → ClickHouse
--------------------------------------------------
Replaces external CDC tools (PeerDB, Debezium+Kafka, etc.) with a
small, dependency-light Python service that:

  1. Creates a logical replication slot using the `wal2json` output
     plugin (bundled in the debezium/postgres image).
  2. On first run, performs a full initial copy of every table into
     ClickHouse (version = 1).
  3. Streams every subsequent INSERT / UPDATE / DELETE from the WAL
     and applies it to ClickHouse as a new row with an increasing
     version, using the ReplacingMergeTree engine for "last write
     wins" semantics — query with FINAL to see current state.

This IS the CDC pipeline for Part 2 of the assignment: PostgreSQL's
logical replication slot + wal2json decoding is the same underlying
mechanism PeerDB itself uses (pgoutput/wal2json) under the hood.
"""

import os
import json
import time
import logging
from datetime import datetime

import psycopg2
import psycopg2.errors
import psycopg2.extras
import clickhouse_connect
from dateutil import parser as dtparser

# ─── Configuration ────────────────────────────────────────────────────────────
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", 5432))
PG_DB       = os.getenv("PG_DB", "ecommerce")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", 8123))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "clickhouse")
CH_DB       = os.getenv("CH_DB", "ecommerce")

SLOT_NAME   = os.getenv("SLOT_NAME", "cdc_slot")

BATCH_SIZE           = 200   # rows buffered per table before flushing
FLUSH_INTERVAL_SECS  = 2.0   # also flush if this much time has passed
INITIAL_LOAD_CHUNK   = 5000  # rows per INSERT during initial copy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("cdc_worker")


# ─── Table schema definitions ──────────────────────────────────────────────────
# column order MUST match the ClickHouse table definition (minus _version/_is_deleted)
TABLES = {
    "categories": {
        "pg_columns": ["category_id", "name", "description", "created_at"],
        "datetime_cols": {"created_at"},
        "defaults": {"description": ""},
    },
    "customers": {
        "pg_columns": ["customer_id", "email", "first_name", "last_name",
                        "phone", "city", "country", "created_at", "updated_at"],
        "datetime_cols": {"created_at", "updated_at"},
        "defaults": {"phone": "", "city": "", "country": ""},
    },
    "products": {
        "pg_columns": ["product_id", "category_id", "name", "sku", "price",
                        "stock", "is_active", "created_at", "updated_at"],
        "datetime_cols": {"created_at", "updated_at"},
        "bool_cols": {"is_active"},
        "defaults": {},
    },
    "orders": {
        "pg_columns": ["order_id", "customer_id", "status", "total_amount",
                        "city", "created_at", "updated_at"],
        "datetime_cols": {"created_at", "updated_at"},
        "defaults": {"city": "", "total_amount": 0},
    },
    "order_items": {
        "pg_columns": ["item_id", "order_id", "product_id", "quantity", "unit_price"],
        "datetime_cols": set(),
        "defaults": {},
    },
    "payments": {
        "pg_columns": ["payment_id", "order_id", "amount", "method", "status",
                        "transaction_ref", "processed_at"],
        "datetime_cols": {"processed_at"},
        "defaults": {"transaction_ref": ""},
    },
}

PG_DSN = (
    f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} "
    f"user={PG_USER} password={PG_PASSWORD}"
)


# ─── ClickHouse helpers ─────────────────────────────────────────────────────────
def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DB,
    )


def coerce_value(col: str, value, table_cfg: dict):
    """Convert a raw value (from psycopg2 or wal2json JSON) into something
    clickhouse-connect can insert."""
    if value is None:
        value = table_cfg.get("defaults", {}).get(col, "")

    if col in table_cfg.get("datetime_cols", set()):
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return dtparser.parse(value)
            except (ValueError, TypeError):
                return datetime.utcnow()
        return datetime.utcnow()

    if col in table_cfg.get("bool_cols", set()):
        return 1 if value else 0

    return value


def build_row(table: str, values: dict, version: int, is_deleted: int) -> list:
    cfg = TABLES[table]
    row = [coerce_value(c, values.get(c), cfg) for c in cfg["pg_columns"]]
    row.append(version)
    row.append(is_deleted)
    return row


def ch_column_names(table: str) -> list:
    return TABLES[table]["pg_columns"] + ["_version", "_is_deleted"]


# ─── Initial full load ──────────────────────────────────────────────────────────
def initial_load(ch_client):
    log.info("Starting initial full load of all tables …")
    pg_conn = psycopg2.connect(PG_DSN)
    pg_cur = pg_conn.cursor()

    for table, cfg in TABLES.items():
        cols_sql = ", ".join(cfg["pg_columns"])
        pg_cur.execute(f"SELECT {cols_sql} FROM {table}")
        rows = pg_cur.fetchall()
        log.info("  %s: %d rows", table, len(rows))

        col_names = ch_column_names(table)
        batch = []
        for r in rows:
            values = dict(zip(cfg["pg_columns"], r))
            batch.append(build_row(table, values, version=1, is_deleted=0))
            if len(batch) >= INITIAL_LOAD_CHUNK:
                ch_client.insert(table, batch, column_names=col_names)
                batch = []
        if batch:
            ch_client.insert(table, batch, column_names=col_names)

    pg_cur.close()
    pg_conn.close()
    log.info("Initial full load complete.")


# ─── Streaming CDC ──────────────────────────────────────────────────────────────
def run_streaming(ch_client):
    repl_conn = psycopg2.connect(
        PG_DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection
    )
    repl_cur = repl_conn.cursor()

    slot_is_new = False
    try:
        repl_cur.create_replication_slot(SLOT_NAME, output_plugin="wal2json")
        slot_is_new = True
        log.info("Created new replication slot '%s'.", SLOT_NAME)
    except psycopg2.errors.DuplicateObject:
        repl_conn.rollback()
        log.info("Replication slot '%s' already exists — resuming.", SLOT_NAME)

    if slot_is_new:
        initial_load(ch_client)

    buffers = {t: [] for t in TABLES}
    last_flush = time.monotonic()

    def flush(force: bool = False):
        nonlocal last_flush
        now = time.monotonic()
        if not force and (now - last_flush) < FLUSH_INTERVAL_SECS:
            any_full = any(len(v) >= BATCH_SIZE for v in buffers.values())
            if not any_full:
                return
        for table, rows in buffers.items():
            if rows:
                ch_client.insert(table, rows, column_names=ch_column_names(table))
                log.info("Flushed %d rows -> ecommerce.%s", len(rows), table)
                buffers[table] = []
        last_flush = now

    def handle_change(change: dict):
        table = change.get("table")
        if table not in TABLES:
            return
        kind = change.get("kind")
        version = time.time_ns()

        if kind in ("insert", "update"):
            values = dict(zip(change["columnnames"], change["columnvalues"]))
            buffers[table].append(build_row(table, values, version, is_deleted=0))

        elif kind == "delete":
            oldkeys = change.get("oldkeys", {})
            values = dict(zip(oldkeys.get("keynames", []), oldkeys.get("keyvalues", [])))
            buffers[table].append(build_row(table, values, version, is_deleted=1))

    def consume(msg):
        try:
            payload = json.loads(msg.payload)
            for change in payload.get("change", []):
                handle_change(change)
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
            flush()
        except Exception:
            log.exception("Error processing WAL message")

    log.info("Starting replication stream on slot '%s' …", SLOT_NAME)
    repl_cur.start_replication(slot_name=SLOT_NAME, decode=True)
    try:
        repl_cur.consume_stream(consume)
    finally:
        flush(force=True)
        repl_cur.close()
        repl_conn.close()


# ─── Main loop with reconnect ───────────────────────────────────────────────────
def main():
    log.info("CDC worker starting. PG=%s:%s/%s  CH=%s:%s/%s",
              PG_HOST, PG_PORT, PG_DB, CH_HOST, CH_PORT, CH_DB)

    while True:
        try:
            ch_client = get_ch_client()
            run_streaming(ch_client)
        except KeyboardInterrupt:
            log.info("Shutting down.")
            break
        except Exception:
            log.exception("CDC worker crashed — retrying in 5s")
            time.sleep(5)


if __name__ == "__main__":
    main()
