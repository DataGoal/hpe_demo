# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 · merge_silver
# MAGIC Applies the latest change per booking from `bronze_booking_updates` to `silver_bookings` with **MERGE** (upsert).
# MAGIC Idempotent: re-running with the same bronze data changes nothing, because only *newer* updates win.

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

rows_ingested = dbutils.jobs.taskValues.get(taskKey="ingest_bronze", key="rows_ingested", default=-1, debugValue=-1)
print("Rows ingested by upstream task:", rows_ingested)

# COMMAND ----------

# First run safety net: seed silver from the current bookings snapshot
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_bookings AS
SELECT booking_id, user_id, property_id, check_in, check_out, guests_count,
       CAST(total_amount AS DOUBLE) AS total_amount, status, created_at, updated_at
FROM samples.wanderbricks.bookings""")

result = spark.sql("""
MERGE INTO silver_bookings AS t
USING (
  SELECT * FROM bronze_booking_updates
  QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY updated_at DESC, booking_update_id DESC) = 1
) AS s
ON t.booking_id = s.booking_id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
  t.check_in = s.check_in, t.check_out = s.check_out, t.guests_count = s.guests_count,
  t.total_amount = s.total_amount, t.status = s.status, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT
  (booking_id, user_id, property_id, check_in, check_out, guests_count, total_amount, status, created_at, updated_at)
  VALUES (s.booking_id, s.user_id, s.property_id, s.check_in, s.check_out, s.guests_count, s.total_amount, s.status, s.created_at, s.updated_at)
""")
display(result)
