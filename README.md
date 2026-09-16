# Real-Time Analytics with PostgreSQL CDC & ClickHouse

> U1T01 — Trends in Data Science · UPY 9no semestre

## Architecture

```
┌──────────────┐   logical replication    ┌──────────────┐
│  PostgreSQL  │ ───── (wal2json) ───────► │  cdc_worker  │
│  (OLTP)      │                           │  (Python)    │
│  port 5432   │                           └──────┬───────┘
└──────────────┘                                  │
       ▲                                          │ writes via
       │ INSERT / UPDATE / DELETE                 │ HTTP :8123
┌──────────────┐                                  ▼
│  generator   │                           ┌──────────────┐
│  (Python)    │                           │  ClickHouse  │
└──────────────┘                           │  (OLAP)      │
                                            └──────────────┘
```

Only **4 containers**, no external CDC tool, no UI to configure, no manual steps.
The CDC pipeline is a ~250-line Python service (`cdc_worker/cdc_worker.py`) that:

1. Opens a **logical replication slot** on PostgreSQL using the `wal2json` output
   plugin, installed via the official `postgresql-15-wal2json` apt package on
   top of the standard `postgres:15` image (see `postgres/Dockerfile`).
2. On first run, does a **full initial copy** of all 6 tables into ClickHouse.
3. From then on, **streams every INSERT/UPDATE/DELETE** from the WAL in real
   time and applies it to ClickHouse using the `ReplacingMergeTree` engine
   ("last write wins" — query with `FINAL` to see current state).

This is the same underlying mechanism (PostgreSQL logical decoding) that tools
like PeerDB or Debezium use — we're just doing it directly, which is more
reliable in this environment and closer to "how CDC works under the hood",
exactly what the assignment asks to explore. The assignment explicitly allows
using any open-source approach, not only PeerDB.

### Services
| Container | Image | Port | Purpose |
|---|---|---|---|
| `oltp_postgres` | postgres:15 + wal2json (local build) | 5432 | OLTP source |
| `olap_clickhouse` | clickhouse/clickhouse-server:24.3 | 8123, 9000 | OLAP target |
| `cdc_worker` | (local build) | — | The CDC pipeline |
| `ecommerce_generator` | (local build) | — | Continuous data generator |

---

## Part 1 — ER Model (3NF)

```
categories ──────────────────────────────── 1
    category_id PK                           │
    name UNIQUE                              │ (FK)
    description                              │
    created_at                               ▼
                                         products ─────────────── 1
                                             product_id PK         │
                                             category_id FK         │ (FK)
                                             name                   │
                                             sku UNIQUE             ▼
                                             price              order_items
                                             stock                  item_id PK
                                             is_active              order_id FK
                                             created_at             product_id FK
                                             updated_at             quantity
                                                                    unit_price
customers ──────────────────────── 1          │
    customer_id PK                  │ (FK)    │ (FK)
    email UNIQUE                    ▼         │
    first_name                  orders ───────┘
    last_name                       order_id PK
    phone                           customer_id FK
    city                            status CHECK(...)
    country                         total_amount
    created_at                      city
    updated_at                      created_at
                                    updated_at
                                        │
                                        │ (FK, 1:1)
                                        ▼
                                    payments
                                        payment_id PK
                                        order_id FK
                                        amount
                                        method CHECK(...)
                                        status CHECK(...)
                                        transaction_ref UNIQUE
                                        processed_at
```

**Why 3NF?**
- `category_id` lives in `products`, not the category name → no transitive dependency.
- `unit_price` is stored in `order_items` (price at time of purchase), not derived from `products.price`.
- Payment attributes (`method`, `status`) live in `payments`, not repeated on `orders`.

---

## Quick Start (single command)

### Prerequisites
- Docker Desktop 24+
- Docker Compose v2+

### If you have a previous version of this project running
Wipe it completely first (old volumes are incompatible with this version):
```bash
docker compose down -v
```

### Run everything
```bash
cd "C:\Users\Diego\Documents\Carrera\UPY\9no\Trends in Data science\act01"
docker compose up --build -d
```

That's it. No browser, no UI, no manual peer/mirror configuration. Give it
about 60-90 seconds for PostgreSQL and ClickHouse to become healthy, the
`cdc_worker` to create its replication slot and finish the initial load, and
the generator to start pumping data.

### Check everything is running
```bash
docker compose ps
```
All 4 containers should show `Up` (postgres and clickhouse should say `healthy`).

### Watch the CDC worker do its thing
```bash
docker logs cdc_worker -f
```
You should see `Created new replication slot`, then `Starting initial full
load of all tables …`, then repeated `Flushed N rows -> ecommerce.<table>`
lines as the generator keeps producing data.

---

## Part 3 — Verify CDC is Working

### Compare row counts
```bash
docker exec -it oltp_postgres psql -U postgres -d ecommerce -c "SELECT COUNT(*) FROM orders;"
```
```bash
docker exec -it olap_clickhouse clickhouse-client --password clickhouse --query "SELECT COUNT(*) FROM ecommerce.orders FINAL WHERE _is_deleted = 0;"
```
The two counts should be close (ClickHouse lags by at most a couple seconds
behind PostgreSQL, since the worker flushes every 2 seconds).

### Verify an UPDATE propagates
```bash
docker exec -it oltp_postgres psql -U postgres -d ecommerce -c "UPDATE products SET price = 999.99 WHERE product_id = 1;"
```
Wait 2-3 seconds, then:
```bash
docker exec -it olap_clickhouse clickhouse-client --password clickhouse --query "SELECT product_id, price FROM ecommerce.products FINAL WHERE product_id = 1;"
```
`price` should show `999.99`.

---

## Part 4 — Benchmarking

### PostgreSQL
```bash
docker exec -it oltp_postgres psql -U postgres -d ecommerce -f /dev/stdin < queries/bench_postgres.sql
```

### ClickHouse
```bash
docker exec -i olap_clickhouse clickhouse-client --password clickhouse --multiquery < queries/bench_clickhouse.sql
```

Capture execution time and scan type (Sequential/Index Scan in PG vs
MergeTree scan in CH) for the report.

---

## Stopping Everything
```bash
docker compose down          # stops containers, keeps data
docker compose down -v       # stops and DELETES all data (fresh start)
```

---

## Repository Structure
```
act01/
├── docker-compose.yml          # 4 services, single command
├── .gitignore
├── README.md
├── postgres/
│   └── init.sql                # Schema (3NF) + seed + REPLICA IDENTITY FULL
├── clickhouse/
│   └── init.sql                # ReplacingMergeTree target tables
├── cdc_worker/
│   ├── Dockerfile
│   ├── cdc_worker.py            # The CDC pipeline itself
│   └── requirements.txt
├── generator/
│   ├── Dockerfile
│   ├── generator.py            # Continuous INSERT/UPDATE/DELETE
│   └── requirements.txt
└── queries/
    ├── bench_postgres.sql      # 5 analytical queries with EXPLAIN ANALYZE
    └── bench_clickhouse.sql    # Same 5 queries adapted for ClickHouse
```

## Notes for the report

Mention that CDC was implemented via **direct PostgreSQL logical decoding**
(`wal2json` plugin + a custom Python consumer) rather than an external tool
like PeerDB. This is explicitly allowed by the assignment ("you are allowed
to use any other open-source tool available") and demonstrates the mechanism
those tools are built on: a replication slot on the source database streams
every row-level change as it's written to the WAL, and a consumer applies it
to the destination — no polling, no scheduled batch jobs.
