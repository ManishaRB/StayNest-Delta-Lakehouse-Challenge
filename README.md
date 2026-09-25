# StayNest Delta Lakehouse Challenge

**Delta Lake & Lakehouse Engineering**
Codebasics · Data Engineering Bootcamp

## Overview

StayNest is a hotel booking platform whose PySpark pipeline is being made
production-ready with **Delta Lake** and a **bronze / silver / gold**
lakehouse design. This project loads raw StayNest CSV data, builds Delta
tables with full history and rollback, tunes them for query performance,
layers them into bronze → silver → gold, and applies a daily incremental
load with `MERGE`.

Runs on **Databricks Free Edition** (serverless) — history, time travel,
`RESTORE`, `OPTIMIZE`, `ZORDER`, and `MERGE` all work natively there.

## Data Assets

| File | Rows | Grain | Key columns |
|---|---|---|---|
| `bookings.csv` | 12,000 | One row per booking | `booking_id`, `customer_id`, `hotel_id`, `booking_date`, `city`, `nights`, `amount`, `status` |
| `hotels.csv` | 200 | One row per hotel | `hotel_id`, `hotel_name`, `city`, `category`, `star_rating` |
| `customers.csv` | 2,000 | One row per customer | `customer_id`, `customer_name`, `city`, `signup_date`, `membership` |
| `bookings_updates.csv` | 200 | Daily change feed | Same schema as `bookings.csv` — 150 changed rows + 50 new rows (IDs from `9100000`) |

> **Note:** `bookings` and `hotels` both carry a `city` column. When joining
> them, drop one side's `city` (e.g. `hotels_df.drop("city")`) to avoid a
> duplicate column.

> **Convention:** `status = 'completed'` is the revenue-recognized state,
> used for all revenue aggregations in silver and gold.

## Task Map (per notebook section)

| # | Task | Delta / Spark feature |
|---|---|---|
| 0 | Load raw data | `spark.read.csv` |
| 1 | Read a query plan, force a broadcast join | `.explain()`, `broadcast()` |
| 2 | Create a Delta table, view history | `DeltaTable`, `DESCRIBE HISTORY` |
| 3 | Time travel | `VERSION AS OF` / `TIMESTAMP AS OF` |
| 4 | Roll back a mistake | `RESTORE TABLE` |
| 5 | Compact small files, cluster for speed | `OPTIMIZE ... ZORDER BY` |
| 6 | Build bronze layer | raw ingest as Delta |
| 7 | Build silver layer | join, clean, dedupe |
| 8 | Build gold layer | business aggregates |
| 9 | Incremental daily load | `MERGE INTO` with `bookings_updates.csv` |

