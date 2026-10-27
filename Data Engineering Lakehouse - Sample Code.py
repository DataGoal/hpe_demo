# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Data Engineer Utility Demo
# MAGIC
# MAGIC A hands-on reference notebook for data engineers covering everyday Databricks commands:
# MAGIC
# MAGIC 1. Parameters (widgets) and environment info
# MAGIC 2. `dbutils` basics (fs, secrets, help)
# MAGIC 3. Mount points on ADLS Gen2 (legacy) and the Unity Catalog alternative (external locations / volumes)
# MAGIC 4. Reading files from an `abfss://` path
# MAGIC 5. `display()` and DataFrame exploration
# MAGIC 6. Creating a Unity Catalog schema and managed Delta table
# MAGIC 7. Table optimization: `OPTIMIZE`, Z-ORDER / Liquid Clustering, `ANALYZE`, `VACUUM`
# MAGIC 8. Delta utilities: `DESCRIBE HISTORY`, time travel, `RESTORE`
# MAGIC 9. Views: standard, temporary, and materialized
# MAGIC 10. Cleanup
# MAGIC
# MAGIC **How to use:** set the widgets at the top, then run cells top to bottom. Cells that require external setup (service principal, storage account) are clearly marked.
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. Parameters (widgets)
# Widgets make the notebook reusable - change values at the top of the notebook UI
dbutils.widgets.text("catalog", "main", "UC Catalog")
dbutils.widgets.text("schema", "de_demo", "UC Schema")
dbutils.widgets.text("storage_account", "<storage-account-name>", "ADLS Storage Account")
dbutils.widgets.text("container", "<container-name>", "ADLS Container")
dbutils.widgets.text("file_path", "raw/sample/sales.csv", "File path inside container")

catalog         = dbutils.widgets.get("catalog")
schema          = dbutils.widgets.get("schema")
storage_account = dbutils.widgets.get("storage_account")
container       = dbutils.widgets.get("container")
file_path       = dbutils.widgets.get("file_path")

abfss_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{file_path}"
print(f"Catalog.Schema : {catalog}.{schema}")
print(f"ABFSS path     : {abfss_path}")

# COMMAND ----------

# DBTITLE 1,Environment info
# Basic session / environment information
print("Spark version :", spark.version)
print("Current user  :", spark.sql("SELECT current_user()").first()[0])
print("Current catalog/schema:", spark.sql("SELECT current_catalog(), current_schema()").first())

# List catalogs you have access to
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `dbutils` basics
# MAGIC `dbutils` provides file system, secrets, widgets, and notebook utilities. Use `dbutils.help()` or `dbutils.fs.help()` to discover commands.

# COMMAND ----------

# DBTITLE 1,dbutils.fs and secrets
# File system help and listing
dbutils.fs.help()

# List a Unity Catalog volume (replace with your volume) or a sample path
# display(dbutils.fs.ls("/Volumes/<catalog>/<schema>/<volume>/"))

# Secrets: list scopes and keys (values are always redacted in output)
for s in dbutils.secrets.listScopes():
    print("Secret scope:", s.name)
# dbutils.secrets.list("<scope-name>")
# my_secret = dbutils.secrets.get(scope="<scope-name>", key="<key-name>")

# COMMAND ----------

# DBTITLE 1,Unity Catalog alternative: external locations and volumes
# MAGIC %sql
# MAGIC -- Recommended over mounts: governed access to ADLS via Unity Catalog
# MAGIC SHOW STORAGE CREDENTIALS;
# MAGIC SHOW EXTERNAL LOCATIONS;
# MAGIC
# MAGIC -- Example (requires CREATE EXTERNAL LOCATION privilege and an existing storage credential):
# MAGIC -- CREATE EXTERNAL LOCATION IF NOT EXISTS de_demo_raw
# MAGIC --   URL 'abfss://<container>@<storage-account>.dfs.core.windows.net/raw'
# MAGIC --   WITH (STORAGE CREDENTIAL <credential_name>);
# MAGIC
# MAGIC -- Example external volume over that location:
# MAGIC -- CREATE EXTERNAL VOLUME IF NOT EXISTS main.de_demo.raw_files
# MAGIC --   LOCATION 'abfss://<container>@<storage-account>.dfs.core.windows.net/raw';

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## 4. Read a file from an ABFSS path
# MAGIC With Unity Catalog, reading `abfss://` works directly when an external location covers the path and you have `READ FILES` on it. Without UC, set the storage account OAuth configs on the Spark session first.

# COMMAND ----------

# DBTITLE 1,Read CSV from ABFSS
# Read a CSV from ADLS Gen2 using the abfss:// path built from the widgets
abfss_path='abfss://container@traningstorage1.dfs.core.windows.net/HPE/taxi_zone_lookup.csv'
if "<" in abfss_path:
    print("Set the storage_account / container / file_path widgets to run this cell.")
else:
    df_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(abfss_path)
    )
    print("Row count:", df_raw.count())
    df_raw.printSchema()
    display(df_raw.limit(10))

# Other formats:
# spark.read.parquet("abfss://...")
# spark.read.json("abfss://...")
# spark.read.format("delta").load("abfss://...")
# spark.read.csv("/Volumes/<catalog>/<schema>/<volume>/file.csv", header=True)   # UC volume
# spark.read.csv(f"/mnt/{container}/{file_path}", header=True)                   # legacy mount

# COMMAND ----------

# DBTITLE 1,Read files with SQL (read_files)
# MAGIC %sql
# MAGIC -- SQL alternative: query files directly without creating a table
# MAGIC -- SELECT * FROM read_files(
# MAGIC --   'abfss://<container>@<storage-account>.dfs.core.windows.net/raw/sample/sales.csv',
# MAGIC --   format => 'csv', header => true
# MAGIC -- ) LIMIT 10;
# MAGIC
# MAGIC -- Runnable example using built-in sample data
# MAGIC SELECT * FROM samples.nyctaxi.trips LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Create a Unity Catalog table from an ABFSS CSV file
# MAGIC Loads `taxi_zone_lookup.csv` from ADLS Gen2 into the managed table `demo-catalog.karthikeyan_arum.nys_trips`.
# MAGIC
# MAGIC * The catalog name contains a hyphen, so it must be wrapped in backticks: `` `demo-catalog` ``.
# MAGIC * The ABFSS path must be covered by a Unity Catalog external location you have `READ FILES` on (or the cluster must have storage credentials configured).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a managed Delta table from the CSV file on ADLS Gen2
# MAGIC CREATE TABLE IF NOT EXISTS `demo-catalog`.karthikeyan_arum.nyc_trips
# MAGIC COMMENT 'NYC taxi zone lookup loaded from ABFSS'
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC   'abfss://container@traningstorage1.dfs.core.windows.net/HPE/taxi_zone_lookup.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );
# MAGIC
# MAGIC -- Verify the load
# MAGIC SELECT COUNT(*) AS row_count FROM `demo-catalog`.karthikeyan_arum.nyc_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE `demo-catalog`.karthikeyan_arum.nyc_trips;
# MAGIC
# MAGIC SELECT * FROM `demo-catalog`.karthikeyan_arum.nyc_trips LIMIT 10;

# COMMAND ----------

# DBTITLE 1,5. display() and DataFrame exploration
# Use the built-in sample dataset so the rest of the demo runs without ADLS access
df = spark.table("samples.nyctaxi.trips")

display(df.limit(20))          # interactive grid with charts / profile tabs
df.printSchema()               # schema
display(df.describe())         # summary statistics

# Simple transformation + aggregation
from pyspark.sql import functions as F
df_agg = (
    df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
      .groupBy("pickup_date")
      .agg(F.count("*").alias("trips"),
           F.round(F.sum("fare_amount"), 2).alias("total_fare"),
           F.round(F.avg("trip_distance"), 2).alias("avg_distance"))
      .orderBy("pickup_date")
)
display(df_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create a Unity Catalog schema and managed Delta table
# MAGIC Uses the three-level namespace `catalog.schema.table`. Requires `USE CATALOG` + `CREATE SCHEMA` on the catalog (or an existing schema with `CREATE TABLE`).

# COMMAND ----------

# DBTITLE 1,Create managed table with SQL (CTAS)
# MAGIC %sql
# MAGIC -- Uses the catalog/schema set in the previous cell
# MAGIC CREATE OR REPLACE TABLE `demo-catalog`.karthikeyan_arum.taxi_trip
# MAGIC COMMENT 'Demo copy of NYC taxi trips'
# MAGIC AS
# MAGIC SELECT
# MAGIC   tpep_pickup_datetime,
# MAGIC   tpep_dropoff_datetime,
# MAGIC   CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
# MAGIC   trip_distance,
# MAGIC   fare_amount,
# MAGIC   pickup_zip,
# MAGIC   dropoff_zip
# MAGIC FROM samples.nyctaxi.trips;
# MAGIC
# MAGIC SELECT COUNT(*) AS row_count FROM taxi_trips;

# COMMAND ----------

# DBTITLE 1,Write a table from a DataFrame (Python)
# Save the aggregated DataFrame as a managed UC table
(df_agg.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`demo-catalog`.karthikeyan_arum.daily_trip_summary"))

display(spark.table(f"`demo-catalog`.karthikeyan_arum.daily_trip_summary").limit(10))

# COMMAND ----------

# DBTITLE 1,Inspect table metadata
# MAGIC %sql
# MAGIC SHOW TABLES;
# MAGIC DESCRIBE TABLE EXTENDED taxi_trips;
# MAGIC -- DESCRIBE DETAIL shows file count, size, location, clustering columns
# MAGIC DESCRIBE DETAIL taxi_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Table optimization
# MAGIC * `OPTIMIZE` compacts small files into larger ones.
# MAGIC * `ZORDER BY` co-locates data for faster filtering (legacy layout technique).
# MAGIC * **Liquid Clustering** (`CLUSTER BY`) is the recommended replacement for partitioning + Z-ORDER.
# MAGIC * `ANALYZE TABLE` collects statistics for the query optimizer.
# MAGIC * `VACUUM` removes files no longer referenced by the table (default retention 7 days).
# MAGIC * **Predictive Optimization** can run these automatically for UC managed tables.

# COMMAND ----------

# DBTITLE 1,OPTIMIZE and Z-ORDER
# MAGIC %sql
# MAGIC -- Compact files and co-locate by a frequently filtered column
# MAGIC OPTIMIZE taxi_trips ZORDER BY (pickup_zip);
# MAGIC
# MAGIC -- Collect statistics for the optimizer
# MAGIC ANALYZE TABLE taxi_trips COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# DBTITLE 1,Liquid Clustering (recommended)
# MAGIC %sql
# MAGIC -- Create a table with liquid clustering instead of partitioning/Z-ORDER
# MAGIC CREATE OR REPLACE TABLE 'taxi_trips_clustered'
# MAGIC CLUSTER BY (pickup_date, pickup_zip)
# MAGIC AS SELECT * FROM taxi_trips;
# MAGIC
# MAGIC -- OPTIMIZE triggers incremental clustering
# MAGIC OPTIMIZE taxi_trips_clustered;
# MAGIC
# MAGIC -- Change clustering keys later without rewriting the table, or let Databricks choose:
# MAGIC -- ALTER TABLE taxi_trips_clustered CLUSTER BY (pickup_zip);
# MAGIC -- ALTER TABLE taxi_trips_clustered CLUSTER BY AUTO;
# MAGIC
# MAGIC DESCRIBE DETAIL taxi_trips_clustered;

# COMMAND ----------

# DBTITLE 1,Table properties and VACUUM
# MAGIC %sql
# MAGIC -- Useful Delta table properties
# MAGIC ALTER TABLE taxi_trips SET TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC SHOW TBLPROPERTIES taxi_trips;
# MAGIC
# MAGIC -- Preview files that would be removed, then vacuum (default 7-day retention)
# MAGIC VACUUM taxi_trips DRY RUN;
# MAGIC -- VACUUM taxi_trips;
# MAGIC
# MAGIC -- Enable predictive optimization at schema level (requires privileges):
# MAGIC -- ALTER SCHEMA de_demo ENABLE PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# DBTITLE 1,8. Delta history, time travel, restore
# MAGIC %sql
# MAGIC -- Audit trail of every write/optimize on the table
# MAGIC DESCRIBE HISTORY taxi_trips;
# MAGIC
# MAGIC -- Make a change, then time travel back
# MAGIC DELETE FROM taxi_trips WHERE fare_amount <= 0;
# MAGIC
# MAGIC SELECT 'version_0' AS v, COUNT(*) AS cnt FROM taxi_trips VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'current', COUNT(*) FROM taxi_trips;
# MAGIC
# MAGIC -- Roll back if needed
# MAGIC -- RESTORE TABLE taxi_trips TO VERSION AS OF 0;

# COMMAND ----------

# DBTITLE 1,8. Delta history, time travel, restore
# MAGIC %sql
# MAGIC -- Audit trail of every write/optimize on the table
# MAGIC DESCRIBE HISTORY taxi_trips;
# MAGIC
# MAGIC -- Make a change, then time travel back
# MAGIC DELETE FROM taxi_trips WHERE fare_amount <= 0;
# MAGIC
# MAGIC SELECT 'version_0' AS v, COUNT(*) AS cnt FROM taxi_trips VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT 'current', COUNT(*) FROM taxi_trips;
# MAGIC
# MAGIC -- Roll back if needed
# MAGIC -- RESTORE TABLE taxi_trips TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8b. Time travel on an existing table: UPDATE, then roll back
# MAGIC Steps:
# MAGIC 1. Record the current table version (the version before the change).
# MAGIC 2. Run an `UPDATE`, which creates a new version.
# MAGIC 3. Compare the old and new data with `VERSION AS OF` / `TIMESTAMP AS OF`.
# MAGIC 4. Undo the change with `RESTORE TABLE ... TO VERSION AS OF <previous version>`.
# MAGIC
# MAGIC Set `tt_table` in the next cell to point at any existing Delta table. By default it uses the `taxi_trips` table created above. Note: `RESTORE` also undoes any other writes made after that version, so on shared tables restore only to a version you have checked.

# COMMAND ----------

# DBTITLE 1,Step 1: capture current version
# Point this at any existing Delta table you want to demo time travel on
tt_table = f"{catalog}.{schema}.taxi_trips"

history_df = spark.sql(f"DESCRIBE HISTORY {tt_table}")
display(history_df.select("version", "timestamp", "operation", "operationParameters", "userName").limit(10))

# Latest version = the state before our UPDATE
version_before = history_df.agg({"version": "max"}).first()[0]
print(f"Version before update: {version_before}")

# COMMAND ----------

# DBTITLE 1,Step 2: UPDATE the table (creates a new version)
# Example change: increase fares by 10% for long trips
update_result = spark.sql(f"""
    UPDATE {tt_table}
    SET fare_amount = ROUND(fare_amount * 1.10, 2)
    WHERE trip_distance > 20
""")
display(update_result)   # shows num_affected_rows

version_after = spark.sql(f"DESCRIBE HISTORY {tt_table} LIMIT 1").first()["version"]
print(f"Version after update: {version_after}")

# COMMAND ----------

# DBTITLE 1,Step 3: compare versions with time travel
# Same rows, before vs after, using VERSION AS OF
compare_df = spark.sql(f"""
    SELECT 'before (v{version_before})' AS version_label,
           COUNT(*) AS long_trips, ROUND(SUM(fare_amount), 2) AS total_fare
    FROM {tt_table} VERSION AS OF {version_before}
    WHERE trip_distance > 20
    UNION ALL
    SELECT 'after (v{version_after})',
           COUNT(*), ROUND(SUM(fare_amount), 2)
    FROM {tt_table} VERSION AS OF {version_after}
    WHERE trip_distance > 20
""")
display(compare_df)

# Time travel by timestamp works the same way:
ts_before = spark.sql(f"DESCRIBE HISTORY {tt_table}").filter(f"version = {version_before}").first()["timestamp"]
print(f"Timestamp of version {version_before}: {ts_before}")
display(spark.sql(f"SELECT * FROM {tt_table} TIMESTAMP AS OF '{ts_before}' WHERE trip_distance > 20 LIMIT 5"))

# DataFrame API equivalents:
# spark.read.option("versionAsOf", version_before).table(tt_table)
# spark.read.option("timestampAsOf", str(ts_before)).table(tt_table)

# COMMAND ----------

# DBTITLE 1,Step 4: RESTORE to the previous version
# Roll the table back to the version captured in Step 1
display(spark.sql(f"RESTORE TABLE {tt_table} TO VERSION AS OF {version_before}"))
# Alternative: RESTORE TABLE <table> TO TIMESTAMP AS OF '<timestamp>'

# Verify: current data should match the pre-update version
display(spark.sql(f"""
    SELECT 'current (restored)' AS version_label,
           COUNT(*) AS long_trips, ROUND(SUM(fare_amount), 2) AS total_fare
    FROM {tt_table} WHERE trip_distance > 20
    UNION ALL
    SELECT 'before (v{version_before})', COUNT(*), ROUND(SUM(fare_amount), 2)
    FROM {tt_table} VERSION AS OF {version_before} WHERE trip_distance > 20
"""))

# RESTORE itself is recorded as a new version in history
display(spark.sql(f"DESCRIBE HISTORY {tt_table} LIMIT 3").select("version", "timestamp", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## 9. Views
# MAGIC * **View** - saved query in Unity Catalog, always computed at read time, governed by UC grants.
# MAGIC * **Temporary view** - session-scoped, not stored in UC.
# MAGIC * **Materialized view** - precomputed results that refresh incrementally; runs on serverless / SQL warehouse.
# MAGIC
# MAGIC Tip for view performance: push filters and aggregations into the view, select only needed columns, and build views over optimized/clustered tables.

# COMMAND ----------

# DBTITLE 1,Standard and temporary views
# MAGIC %sql
# MAGIC -- Governed UC view
# MAGIC CREATE OR REPLACE VIEW v_daily_zip_fares
# MAGIC COMMENT 'Daily fares by pickup zip'
# MAGIC AS
# MAGIC SELECT pickup_date, pickup_zip,
# MAGIC        COUNT(*)                   AS trips,
# MAGIC        ROUND(SUM(fare_amount), 2) AS total_fare
# MAGIC FROM taxi_trips_clustered
# MAGIC GROUP BY pickup_date, pickup_zip;
# MAGIC
# MAGIC SELECT * FROM v_daily_zip_fares ORDER BY total_fare DESC LIMIT 10;
# MAGIC
# MAGIC -- Session-scoped temp view
# MAGIC CREATE OR REPLACE TEMP VIEW tv_long_trips AS
# MAGIC SELECT * FROM taxi_trips WHERE trip_distance > 10;
# MAGIC
# MAGIC SELECT COUNT(*) AS long_trips FROM tv_long_trips;

# COMMAND ----------

# DBTITLE 1,Materialized view (optional)
# MAGIC %sql
# MAGIC -- Precomputed view for dashboards / repeated queries (requires serverless or a SQL warehouse)
# MAGIC -- CREATE OR REPLACE MATERIALIZED VIEW mv_daily_zip_fares AS
# MAGIC -- SELECT pickup_date, pickup_zip, COUNT(*) AS trips, SUM(fare_amount) AS total_fare
# MAGIC -- FROM taxi_trips_clustered
# MAGIC -- GROUP BY pickup_date, pickup_zip;
# MAGIC
# MAGIC -- REFRESH MATERIALIZED VIEW mv_daily_zip_fares;
# MAGIC
# MAGIC -- Inspect view definition
# MAGIC SHOW CREATE TABLE v_daily_zip_fares;

# COMMAND ----------

# DBTITLE 1,Grants (Unity Catalog permissions)
# MAGIC %sql
# MAGIC -- Inspect and manage access (replace the principal with a real group)
# MAGIC SHOW GRANTS ON TABLE taxi_trips;
# MAGIC -- GRANT SELECT ON VIEW v_daily_zip_fares TO `data-analysts`;
# MAGIC -- REVOKE SELECT ON VIEW v_daily_zip_fares FROM `data-analysts`;

# COMMAND ----------

# DBTITLE 1,10. Cleanup
# Uncomment to remove demo objects
# spark.sql(f"DROP VIEW IF EXISTS {catalog}.{schema}.v_daily_zip_fares")
# spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.taxi_trips_clustered")
# spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.taxi_trips")
# spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.daily_trip_summary")
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
# dbutils.widgets.removeAll()
print("Cleanup commands are commented out by default.")