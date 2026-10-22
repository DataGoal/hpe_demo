# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 · ingest_bronze
# MAGIC Part of the **Day 2 Wanderbricks bookings pipeline** (created by notebook 02A).
# MAGIC Incrementally ingests new booking-change JSON files from the landing volume into `bronze_booking_updates` using **Auto Loader** (`availableNow` trigger: process everything new, then stop).

# COMMAND ----------

# Job parameters arrive as widgets. Defaults let you also run this notebook by hand.
import re
dbutils.widgets.text("catalog", "demo-catalog")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("run_date", "2025-07-31")

CATALOG = dbutils.widgets.get("catalog")
USER_SCHEMA = dbutils.widgets.get("schema") or re.sub(
    r"[^a-z0-9_]", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0].lower())
RUN_DATE = dbutils.widgets.get("run_date")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{USER_SCHEMA}`")
print(f"catalog={CATALOG} schema={USER_SCHEMA} run_date={RUN_DATE}")

# COMMAND ----------

from datetime import datetime
# Fail fast on bad parameters: a clear error here is much easier to troubleshoot than a strange result downstream
datetime.strptime(RUN_DATE, "%Y-%m-%d")

# COMMAND ----------

from pyspark.sql import functions as F

LANDING = f"/Volumes/{CATALOG}/{USER_SCHEMA}/landing"
CHECKPOINT = f"{LANDING}/_checkpoints/bronze_booking_updates"
HINTS = ("booking_id BIGINT, booking_update_id BIGINT, user_id BIGINT, property_id BIGINT, guests_count INT, "
         "total_amount DOUBLE, check_in DATE, check_out DATE, created_at TIMESTAMP, updated_at TIMESTAMP, status STRING")

query = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
         .option("cloudFiles.schemaHints", HINTS)
         .load(f"{LANDING}/booking_updates")
         .select("*",
                 F.col("_metadata.file_path").alias("_source_file"),
                 F.current_timestamp().alias("_ingested_at"))
         .writeStream
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)
         .toTable(f"`{CATALOG}`.`{USER_SCHEMA}`.bronze_booking_updates"))
query.awaitTermination()

rows = sum(p["numInputRows"] or 0 for p in query.recentProgress)
print(f"Ingested {rows} new rows")
dbutils.jobs.taskValues.set(key="rows_ingested", value=rows)   # visible to downstream tasks and in the run UI
