# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # StayNest - Session 7 Assignment (Delta Lake & Lakehouse)
# MAGIC Work through the 8 tasks in order. Read the Assignment Questions PDF for the full
# MAGIC detail and acceptance criteria. Fill each `# TODO` cell, run it, and keep the output
# MAGIC visible. Runs on Databricks Free Edition (serverless).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 0 - Setup (already done for you)
# MAGIC Upload `bookings.csv`, `hotels.csv`, `bookings_updates.csv` to a Volume, set `BASE`,
# MAGIC `CATALOG`, `SCHEMA`, and run this cell. Expect 12000 / 200 / 200.

# COMMAND ----------

BASE    = "/Volumes/workspace/default/staynest_07"
CATALOG = "staynest07_catalog"
SCHEMA  = "default"
FQN = lambda name: f"{CATALOG}.{SCHEMA}.{name}"

read_csv = lambda name: (spark.read
    .option("header", True).option("inferSchema", True)
    .csv(f"{BASE}/{name}.csv"))

bookings_df = read_csv("bookings")
hotels_df   = read_csv("hotels")
updates_df  = read_csv("bookings_updates")

print(f"bookings: {bookings_df.count()}, hotels: {hotels_df.count()}, "
      f"updates: {updates_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 - Read the plan and force a broadcast join
# MAGIC Join bookings to hotels and call `.explain()` to see the plan. Then force a
# MAGIC broadcast join with `broadcast(hotels_df)` and `.explain()` again. In a comment,
# MAGIC say which join each plan used and why broadcast avoids a shuffle.
# MAGIC (Tip: hotels also has a `city` column, so `hotels_df.drop("city")` before joining.)

# COMMAND ----------

from pyspark.sql.functions import broadcast

# hotels also has a city column, so hotels_df.drop("city") before joining
hotels_slim = hotels_df.drop("city")   # bookings already carries city; avoids a duplicate column

# --- Plan 1:Join bookings to hotels and call .explain() to see the plan ---
plain_join = bookings_df.join(hotels_slim, "hotel_id")
plain_join.explain()

# --- Plan 2: forced broadcast of the small table ---
broadcast_join = bookings_df.join(broadcast(hotels_slim), "hotel_id")
broadcast_join.explain()

# Plan 1 used: <fill in after running: BroadcastHashJoin or SortMergeJoin>.
# Plan 2 used: BroadcastHashJoin. Hotels (200 rows) is copied to every executor
# A broadcast join needs no shuffle because every task already holds the full small
# table and joins its own bookings partition locally; a SortMergeJoin would have to
# shuffle and sort both sides by hotel_id first.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 - Create a Delta table, then read its history
# MAGIC Write `bookings_df` as a managed Delta table with `saveAsTable`. Then create some
# MAGIC history: run an `UPDATE` (set pending to completed) and a `DELETE` (remove
# MAGIC cancelled). Show `DESCRIBE HISTORY` and point out the versioned commits.

# COMMAND ----------

from pyspark.sql import functions as F

target = FQN("bookings_delta")   # workspace.default.bookings_delta

# Check the real status values first (spelling/case must match the WHERE clauses)
bookings_df.groupBy("status").count().show()

# --- Version 0: WRITE ---
spark.sql(f"DROP TABLE IF EXISTS {target}")   # makes the cell safe to re-run from a clean history
(bookings_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")   # allows re-runs even after schema evolution
    .format("delta")
    .saveAsTable(target))

# --- Version 1: UPDATE ---
spark.sql(f"UPDATE {target} SET status = 'completed' WHERE status = 'pending'")

# --- Version 2: DELETE ---
spark.sql(f"DELETE FROM {target} WHERE status = 'cancelled'")

# --- Read the history ---
display(spark.sql(f"DESCRIBE HISTORY {target}")
        .select("version", "timestamp", "operation"))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 - Time travel and RESTORE
# MAGIC Read the table as it was at **version 0** (before your UPDATE and DELETE) and show
# MAGIC its count. Then `RESTORE` the table to version 0 and confirm the count is back.
# MAGIC Show that RESTORE appears as a new commit in the history.

# COMMAND ----------

target = FQN("bookings_delta")

# Current count (after UPDATE + DELETE)
current_count = spark.table(target).count()

# Read version 0 with the DataFrame reader
v0_df = (spark.read
         .format("delta")
         .option("versionAsOf", 0)
         .table(target))

v0_count = v0_df.count()
print(f"Current: {current_count} | Version 0: {v0_count}")   # expect these to differ

# Roll back
spark.sql(f"RESTORE TABLE {target} TO VERSION AS OF 0")

# Confirm the count is back
restored_count = spark.table(target).count()
print("After RESTORE:", restored_count)                      # should equal v0_count

# History again: look for the new RESTORE row on top
display(spark.sql(f"DESCRIBE HISTORY {target}")
        .select("version", "timestamp", "operation")
        .orderBy("version", ascending=False))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 - OPTIMIZE and ZORDER
# MAGIC Run `OPTIMIZE` on your Delta table to compact files. Then run
# MAGIC `OPTIMIZE ... ZORDER BY (city)`. In a comment, say what OPTIMIZE does and why
# MAGIC `city` is a good ZORDER column but `status` would not be.

# COMMAND ----------

target = FQN("bookings_delta")

# File layout before
display(spark.sql(f"DESCRIBE DETAIL {target}").select("numFiles", "sizeInBytes"))

# 1. Compact small files
display(spark.sql(f"OPTIMIZE {target}"))

# 2. Compact and colocate rows by city
display(spark.sql(f"OPTIMIZE {target} ZORDER BY (city)"))

# OPTIMIZE rewrites many small files into fewer, larger ones (bin-packing), which cuts
# per-file overhead when reading. It is a new commit; the old files stay for time travel.

# city is a sensible ZORDER column because it has many distinct values and is likely
# to be used in filters (WHERE city = ...). ZORDER sorts rows by city so each file
# covers a narrow range of cities, and data skipping can then skip files whose
# min/max city stats can't match the filter.
# status would not help: with only three values, nearly every file contains every
# status, so min/max stats can't rule out any file and there is nothing to skip.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 - Bronze: land the raw data
# MAGIC Write the raw bookings to a `bronze_bookings` Delta table, keeping every row and
# MAGIC adding an `ingested_at` timestamp column.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# ── Ingest raw CSV → Bronze Delta with metadata ──
bronze_bookings = (spark.read.csv(f"{BASE}/bookings.csv",
                                header=True, inferSchema=True)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("bookings.csv"))
)

(bronze_bookings.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")   # safe to re-run
    .saveAsTable(FQN("bronze_bookings")))

print(f"✅ Bronze table created: {FQN('bronze_bookings')}")
spark.table(FQN("bronze_bookings")).select(
    "booking_id", "amount", "_ingested_at", "_source_file"
).show(3, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6 - Silver: clean and conform
# MAGIC Build `silver_bookings` from bronze: keep only completed bookings and join the
# MAGIC hotel dimension to add `category`, `star_rating`, and the hotel name. Drop the
# MAGIC duplicate `city` from the hotel side so the join has a single `city`.

# COMMAND ----------

from pyspark.sql.functions import col

# Check the hotel-side column names (category, star_rating and the hotel name column)
print(hotels_df.columns)

silver_df = (spark.table(FQN("bronze_bookings"))
    .filter(col("status") == "completed")
    .join(hotels_df.drop("city"), "hotel_id"))    # bookings keeps its city; hotels' copy is dropped

(silver_df.write
    .mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(FQN("silver_bookings")))

# Verify
s = spark.table(FQN("silver_bookings"))
print("silver rows:", s.count())
print("city columns:", s.columns.count("city"))        # expect 1
print("statuses:", [r[0] for r in s.select("status").distinct().collect()])   # expect ['completed']
assert s.columns.count("city") == 1
# assert s.filter(col("status") != "completed").count() == 0
display(s.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 7 - Gold: business-ready aggregate
# MAGIC From silver, build a `gold_city_revenue` Delta table: bookings and total revenue
# MAGIC per city, ordered by revenue.

# COMMAND ----------

from pyspark.sql.functions import count, sum as _sum, col, desc

SILVER = FQN("silver_bookings")
GOLD   = FQN("gold_city_revenue")

gold_df = (spark.table(SILVER)
    .groupBy("city")
    .agg(count("*").alias("bookings"),
         _sum("amount").alias("revenue"))
    .orderBy(desc("revenue")))

(gold_df.write
    .mode("overwrite").format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD))

# Verify
g = spark.table(GOLD)
display(g.orderBy(desc("revenue")))    # re-sort on read; see note below
print("cities:", g.count())

# Totals should reconcile with silver
silver_total = spark.table(SILVER).agg(_sum("amount")).first()[0]
gold_total = g.agg(_sum("revenue")).first()[0]
print("silver revenue:", silver_total, "| gold revenue:", gold_total)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 8 - Incremental load with MERGE
# MAGIC You have today's batch in `updates_df` (150 changed bookings + 50 new ones).
# MAGIC `MERGE` it into your Delta table: update matched booking_ids, insert new ones, in
# MAGIC one command. Report the row count before and after (it should grow by the 50 new).

# COMMAND ----------

# Roll back to the pre-merge state (version 0, the original load)
spark.sql(f"RESTORE TABLE {FQN("bookings_delta")} TO VERSION AS OF 0")
print("Reset to:", spark.table(FQN("bookings_delta")).count())   # expect 12000
# Sanity checks before merging
print("bookings_delta cols match batch:", set(spark.table(FQN("bookings_delta")).columns) == set(updates_df.columns))
print("batch rows:", updates_df.count(),
      "| distinct booking_ids:", updates_df.select("booking_id").distinct().count())   # expect 200 / 200

updates_df.createOrReplaceTempView("batch")

# Count BEFORE
before = spark.table(FQN("bookings_delta")).count()
print("Before MERGE:", before)        # expect 12000

# One MERGE: update matched, insert new
spark.sql(f"""
    MERGE INTO {FQN("bookings_delta")} AS t
    USING batch AS s
    ON t.booking_id = s.booking_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Count AFTER
after = spark.table(FQN("bookings_delta")).count()
print("After MERGE:", after)          # expect 12050
print("Net new rows:", after - before)
assert after - before == 50

# Prove the split: 150 updated, 50 inserted
m = (spark.sql(f"DESCRIBE HISTORY {FQN("bookings_delta")} LIMIT 1")
        .select("version", "operation", "operationMetrics").first())
print(m["operation"], {k: v for k, v in m["operationMetrics"].items()
                       if k in ("numTargetRowsUpdated", "numTargetRowsInserted", "numTargetRowsDeleted")})
