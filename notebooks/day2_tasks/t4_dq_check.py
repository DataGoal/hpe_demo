# Databricks notebook source
# MAGIC %md
# MAGIC # Task 4 · dq_check
# MAGIC Generic **data-quality gate**: checks that `key_cols` are **unique** and **not null** in `table`.
# MAGIC Every result is appended to `dq_results`. If a check fails, the task **raises an exception** so the job run fails and alerts someone.

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

dbutils.widgets.text("table", "silver_bookings")
dbutils.widgets.text("key_cols", "booking_id")
TABLE = dbutils.widgets.get("table")
KEYS = [k.strip() for k in dbutils.widgets.get("key_cols").split(",")]

# COMMAND ----------

keys = ", ".join(KEYS)
null_cond = " OR ".join(f"{k} IS NULL" for k in KEYS)
stats = spark.sql(f"""
  SELECT COUNT(*)                                        AS row_count,
         COUNT(*) - COUNT(DISTINCT {keys})               AS duplicate_keys,
         COUNT_IF({null_cond})                           AS null_keys
  FROM {TABLE}""").first()

spark.sql("""CREATE TABLE IF NOT EXISTS dq_results (
  checked_at TIMESTAMP, run_date STRING, table_name STRING, key_cols STRING,
  row_count BIGINT, duplicate_keys BIGINT, null_keys BIGINT, passed BOOLEAN)""")
passed = stats.duplicate_keys == 0 and stats.null_keys == 0
spark.sql(f"""INSERT INTO dq_results VALUES (current_timestamp(), '{RUN_DATE}', '{TABLE}', '{keys}',
              {stats.row_count}, {stats.duplicate_keys}, {stats.null_keys}, {passed})""")

print(f"{TABLE}: rows={stats.row_count} duplicate_keys={stats.duplicate_keys} null_keys={stats.null_keys}")
if not passed:
    raise ValueError(f"DQ check FAILED for {TABLE}: {stats.duplicate_keys} duplicate and {stats.null_keys} null values in key ({keys})")
print("✅ DQ check passed")
