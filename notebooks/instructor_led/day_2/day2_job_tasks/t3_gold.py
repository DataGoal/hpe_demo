# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3 · build_gold
# MAGIC Rebuilds `gold_daily_revenue`: bookings, realised revenue and cancellations per booking date and destination.

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

spark.sql(f"""
CREATE OR REPLACE TABLE gold_daily_revenue
COMMENT 'Gold: daily bookings, realised revenue and cancellations by destination'
AS
SELECT TO_DATE(b.created_at)                                                   AS booking_date,
       d.destination,
       COUNT(*)                                                                AS bookings,
       ROUND(SUM(CASE WHEN b.status IN ('confirmed','completed') THEN b.total_amount END), 2) AS realised_revenue,
       COUNT_IF(b.status = 'cancelled')                                        AS cancellations,
       DATE'{RUN_DATE}'                                                        AS pipeline_run_date,
       CURRENT_TIMESTAMP()                                                     AS refreshed_at
FROM silver_bookings b
JOIN samples.wanderbricks.properties   p ON b.property_id = p.property_id
JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
GROUP BY ALL
""")
print("gold_daily_revenue rows:", spark.table("gold_daily_revenue").count())
