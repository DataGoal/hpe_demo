# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02A · Day 2: Build & Manage Workflows
# MAGIC ### Databricks Catalyst · Instructor-led demo
# MAGIC
# MAGIC | Module | Topic | Sections |
# MAGIC |---|---|---|
# MAGIC | 2.1 · Part 1 | Notebook & code authoring: Python/SQL/Markdown, widgets, Databricks Assistant | §1 |
# MAGIC | 2.1 · Part 2 | Ingestion, medallion pipeline, workflow task graphs, schedules, parameters | §2–§7 |
# MAGIC | 2.2 | Workflow monitoring & troubleshooting: runs, logs, stack traces, repair | §8–§9 |
# MAGIC
# MAGIC ### Learning objectives
# MAGIC - Author a **multi-language notebook** with **widgets**, and use the **Assistant** to explain, document and fix code
# MAGIC - Ingest files with the three methods from the deck: **CTAS + `read_files`**, **`COPY INTO`**, **Auto Loader**
# MAGIC - Build a **bronze → silver → gold** medallion pipeline with an idempotent **MERGE**
# MAGIC - Turn notebooks into a **parameterised, scheduled job** with a **task graph**
# MAGIC - **Monitor** runs, read a **stack trace**, and **repair** a failed run
# MAGIC
# MAGIC ### Today's scenario
# MAGIC > *"Every night our booking platform drops a JSON file with all booking **changes** (new bookings, confirmations, cancellations) into cloud storage. Today an analyst runs scripts by hand. Build us a **reliable nightly pipeline** that keeps a clean bookings table and a revenue table up to date, and make sure we know when it breaks."*
# MAGIC > *Head of Data Engineering, Wanderbricks*
# MAGIC
# MAGIC ```
# MAGIC  landing volume (JSON files)  ──Auto Loader──▶  bronze_booking_updates  ──MERGE──▶  silver_bookings  ──▶  gold_daily_revenue
# MAGIC                                                                                               └──▶  dq_check
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: user schema
# MAGIC
# MAGIC Every participant works in an **isolated schema**: `` `demo-catalog`.`<your_username>` ``.
# MAGIC
# MAGIC This cell:
# MAGIC 1. Reads your login with `current_user()` (for example, `jane.doe@company.com`).
# MAGIC 2. Turns the part before `@` into a valid schema name (lower-case; anything that is not a letter, digit or `_` becomes `_`). For example, `jane_doe`.
# MAGIC 3. Creates the schema **if it does not exist**, so the cell is safe to re-run.
# MAGIC 4. Makes it your default catalog and schema, so unqualified names like `bookings_enriched` land in **your** schema.
# MAGIC
# MAGIC > Source data is read **only** from `samples.wanderbricks`. Never write to `samples` or to anyone else's schema.

# COMMAND ----------

import re

CATALOG = "demo-catalog"
SRC = "samples.wanderbricks"

USER_EMAIL = spark.sql("SELECT current_user() AS u").first()["u"]
USER_SCHEMA = re.sub(r"[^a-z0-9_]", "_", USER_EMAIL.split("@")[0].lower())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{USER_SCHEMA}` COMMENT 'Personal schema for Databricks Catalyst training'")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{USER_SCHEMA}`")

FQ = f"`{CATALOG}`.`{USER_SCHEMA}`"   # fully-qualified user schema name, used in f-strings
print(f"User        : {USER_EMAIL}")
print(f"Schema      : {FQ}")
print(f"Source data : {SRC} (read-only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Notebook & code authoring (Module 2.1 · Part 1)
# MAGIC
# MAGIC ### 1a. Widgets = notebook parameters
# MAGIC Widgets add input boxes at the top of the notebook. When the notebook runs as a **job task**, job parameters with the same name fill them in. This is how one notebook serves every run date.

# COMMAND ----------

dbutils.widgets.text("run_date", "2025-07-31", "Run date (YYYY-MM-DD)")
dbutils.widgets.dropdown("demo_errors", "no", ["no", "yes"], "Run the deliberately broken cells?")

RUN_DATE = dbutils.widgets.get("run_date")
print("Pipeline run date:", RUN_DATE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Mixing languages
# MAGIC The notebook's default language is Python. Any cell can switch language with a **magic command**: `%sql`, `%md`, `%sh`, `%run` (and `%scala` / `%r` on classic compute).
# MAGIC Python and SQL share the **same Spark session**, so tables, temp views and the current catalog/schema are visible to both.

# COMMAND ----------

# Python → SQL: register a DataFrame as a temporary view
from pyspark.sql import functions as F

(spark.table(f"{SRC}.bookings")
      .groupBy("status")
      .agg(F.count("*").alias("bookings"), F.round(F.sum("total_amount"), 0).alias("amount"))
      .createOrReplaceTempView("tv_status_summary"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL reads the Python temp view
# MAGIC SELECT status, bookings, amount, ROUND(100 * bookings / SUM(bookings) OVER (), 1) AS pct
# MAGIC FROM tv_status_summary
# MAGIC ORDER BY bookings DESC

# COMMAND ----------

# SQL → Python: run SQL and keep working with the DataFrame
df = spark.sql("SELECT * FROM tv_status_summary WHERE status = 'cancelled'")
print("Cancelled bookings:", df.first()["bookings"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1c. Databricks Assistant (in-notebook AI)
# MAGIC Open the Assistant with the Assistant icon in a cell, or press **Cmd/Ctrl + I** in a cell. Useful slash commands:
# MAGIC
# MAGIC | Command | What it does | Try it on… |
# MAGIC |---|---|---|
# MAGIC | `/explain` | Explains what the selected code does | The MERGE cell in §4 |
# MAGIC | `/doc` | Adds comments / docstrings | The `drop_batch` helper in §2 |
# MAGIC | `/fix` | Diagnoses an error and proposes a fix | The broken cell below |
# MAGIC | `/optimize` | Suggests performance improvements | Any heavy query |
# MAGIC | *Free text* | e.g. "Write SQL for revenue by month for cancelled vs completed bookings" | An empty cell |
# MAGIC
# MAGIC **Instructor demonstration:** set the widget `demo_errors = yes`, run the next cell, and click **"Diagnose error"** / type `/fix`.
# MAGIC The cell contains **two defects** (an incorrect column name and a type mismatch). The Assistant should identify both.

# COMMAND ----------

if dbutils.widgets.get("demo_errors") == "yes":
    # Deliberately broken: two defects for the Assistant to /fix
    nights = (spark.table(f"{SRC}.bookings")
                   .withColumn("nights", F.datediff("check_out", "checkin"))       # bug 1: column is check_in
                   .filter(F.col("nights") > "seven"))                            # bug 2: comparing int to text
    display(nights)
else:
    print("Skipped (set demo_errors = yes to see the error and try /fix)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Simulating the nightly file drops
# MAGIC
# MAGIC `samples.wanderbricks.booking_updates` has 83K booking changes. We split them by time into **5 nightly batches** and write each batch as a JSON file into a **landing volume**, just like an upstream system would.
# MAGIC
# MAGIC The cell below first **resets** the demo (deletes files, checkpoints and bronze tables), so the notebook is safe to re-run from the top.

# COMMAND ----------

LANDING = f"/Volumes/{CATALOG}/{USER_SCHEMA}/landing"

# Reset: re-running the notebook starts from a clean slate
spark.sql("CREATE VOLUME IF NOT EXISTS landing COMMENT 'Landing zone for raw files (Day 2 pipeline)'")
dbutils.fs.rm(f"{LANDING}/booking_updates", recurse=True)
dbutils.fs.rm(f"{LANDING}/_checkpoints", recurse=True)
for t in ["bronze_bu_ctas", "bronze_bu_copy_into", "bronze_booking_updates", "silver_bookings", "gold_daily_revenue"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

# Stage the 5 batches once (NTILE splits rows into 5 equal groups ordered by update time)
spark.sql(f"""
CREATE OR REPLACE TABLE stg_booking_update_batches AS
SELECT *, NTILE(5) OVER (ORDER BY updated_at, booking_update_id) AS batch_no
FROM {SRC}.booking_updates""")
display(spark.sql("SELECT batch_no, COUNT(*) AS rows, MIN(updated_at) AS from_ts, MAX(updated_at) AS to_ts FROM stg_booking_update_batches GROUP BY 1 ORDER BY 1"))

# COMMAND ----------

def drop_batch(n: int) -> str:
    """Write batch n as a single JSON file into the landing folder, like an upstream system's nightly export."""
    tmp = f"{LANDING}/_tmp/batch_{n:02d}"
    (spark.table("stg_booking_update_batches").filter(F.col("batch_no") == n).drop("batch_no")
          .coalesce(1).write.mode("overwrite").json(tmp))
    part = [f.path for f in dbutils.fs.ls(tmp) if f.name.endswith(".json")][0]
    target = f"{LANDING}/booking_updates/booking_updates_batch_{n:02d}.json"
    dbutils.fs.cp(part, target)
    dbutils.fs.rm(tmp, recurse=True)
    return target

for n in (1, 2, 3):
    print("Landed", drop_batch(n))

# COMMAND ----------

# SQL LIST works on volume paths (same as dbutils.fs.ls)
display(spark.sql(f"LIST '{LANDING}/booking_updates'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Three ways to ingest files (deck: *Data Ingestion*)
# MAGIC
# MAGIC | Method | Type | Re-reads old files? | Best for |
# MAGIC |---|---|---|---|
# MAGIC | **CTAS + `read_files`** | Batch | Yes, every time (full reload) | One-off loads, small reference data |
# MAGIC | **`COPY INTO`** | Incremental batch (SQL) | No, skips loaded files | Scheduled SQL loads, thousands of files |
# MAGIC | **Auto Loader** (`cloudFiles`) | Incremental batch **or** streaming | No, the checkpoint tracks files | Millions of files, schema evolution, production |
# MAGIC
# MAGIC ### Method 1: CTAS + `read_files` (batch)

# COMMAND ----------

SCHEMA_HINTS = ("booking_id BIGINT, booking_update_id BIGINT, user_id BIGINT, property_id BIGINT, guests_count INT, "
                "total_amount DOUBLE, check_in DATE, check_out DATE, created_at TIMESTAMP, updated_at TIMESTAMP, status STRING")

spark.sql(f"""
CREATE OR REPLACE TABLE bronze_bu_ctas AS
SELECT *, _metadata.file_name AS source_file
FROM read_files('{LANDING}/booking_updates', format => 'json', schemaHints => '{SCHEMA_HINTS}')
""")
display(spark.sql("SELECT source_file, COUNT(*) AS rows FROM bronze_bu_ctas GROUP BY 1 ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC This approach is simple, but every run **re-reads every file**. With three years of nightly files, it becomes slow and expensive.
# MAGIC
# MAGIC ### Method 2: `COPY INTO` (incremental, idempotent)
# MAGIC `COPY INTO` remembers which files it has loaded, so re-running it is safe.

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS bronze_bu_copy_into")   # schema-less: COPY INTO infers it

def copy_into():
    return spark.sql(f"""
      COPY INTO bronze_bu_copy_into
      FROM '{LANDING}/booking_updates'
      FILEFORMAT = JSON
      FORMAT_OPTIONS ('inferSchema' = 'true')
      COPY_OPTIONS ('mergeSchema' = 'true')""")

display(copy_into())          # 1st run: loads 3 files

# COMMAND ----------

display(copy_into())          # 2nd run: 0 rows, the files were already loaded

# COMMAND ----------

print("Landed", drop_batch(4))   # a new night arrives…
display(copy_into())             # only the new file is loaded

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 3: Auto Loader (production choice) → our real bronze table
# MAGIC Auto Loader (`format("cloudFiles")`) discovers new files efficiently, tracks progress in a **checkpoint**, and handles **schema evolution** (unexpected columns land in `_rescued_data`).
# MAGIC
# MAGIC - `trigger(availableNow=True)` = **incremental batch**: process everything new, then stop. This is perfect for scheduled jobs and is what serverless supports.
# MAGIC - `trigger(processingTime="5 seconds")` = **continuous streaming** (classic compute).
# MAGIC
# MAGIC We also add **audit columns**: the source file and the ingestion time.

# COMMAND ----------

CHECKPOINT = f"{LANDING}/_checkpoints/bronze_booking_updates"

def run_auto_loader():
    q = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/schema")
         .option("cloudFiles.schemaHints", SCHEMA_HINTS)
         .load(f"{LANDING}/booking_updates")
         .select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at"))
         .writeStream
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)
         .toTable(f"{FQ}.bronze_booking_updates"))
    q.awaitTermination()
    return sum(p["numInputRows"] or 0 for p in q.recentProgress)

print("Rows ingested:", run_auto_loader())

# COMMAND ----------

print("Rows ingested on re-run:", run_auto_loader())   # 0: the checkpoint remembers the files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _source_file, COUNT(*) AS rows, MIN(_ingested_at) AS ingested_at
# MAGIC FROM bronze_booking_updates
# MAGIC GROUP BY ALL ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC > The same ingestion in **Lakeflow Declarative Pipelines** is one SQL statement:
# MAGIC > ```sql
# MAGIC > CREATE OR REFRESH STREAMING TABLE bronze_booking_updates
# MAGIC > SCHEDULE EVERY 1 HOUR
# MAGIC > AS SELECT * FROM STREAM read_files('/Volumes/<catalog>/<schema>/landing/booking_updates', format => 'json');
# MAGIC > ```
# MAGIC
# MAGIC ---
# MAGIC ## 4. Silver: apply the changes with MERGE
# MAGIC Bronze holds **every change** (several rows per booking). Silver must hold the **current state**: one row per booking.
# MAGIC
# MAGIC 1. Seed silver from today's bookings snapshot.
# MAGIC 2. Keep only the **latest change per booking** (`QUALIFY ROW_NUMBER() … = 1`).
# MAGIC 3. **MERGE**: update if the change is newer; insert if the booking is new (this covers the 47 orphan updates identified in notebook 00).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_bookings
# MAGIC COMMENT 'Silver: current state of every booking (one row per booking_id)'
# MAGIC AS
# MAGIC SELECT booking_id, user_id, property_id, check_in, check_out, guests_count,
# MAGIC        CAST(total_amount AS DOUBLE) AS total_amount, status, created_at, updated_at
# MAGIC FROM samples.wanderbricks.bookings;
# MAGIC
# MAGIC SELECT status, COUNT(*) AS bookings FROM silver_bookings GROUP BY status ORDER BY status;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_bookings AS t
# MAGIC USING (
# MAGIC   SELECT * FROM bronze_booking_updates
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY updated_at DESC, booking_update_id DESC) = 1
# MAGIC ) AS s
# MAGIC ON t.booking_id = s.booking_id
# MAGIC WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
# MAGIC   t.check_in = s.check_in, t.check_out = s.check_out, t.guests_count = s.guests_count,
# MAGIC   t.total_amount = s.total_amount, t.status = s.status, t.updated_at = s.updated_at
# MAGIC WHEN NOT MATCHED THEN INSERT
# MAGIC   (booking_id, user_id, property_id, check_in, check_out, guests_count, total_amount, status, created_at, updated_at)
# MAGIC   VALUES (s.booking_id, s.user_id, s.property_id, s.check_in, s.check_out, s.guests_count, s.total_amount, s.status, s.created_at, s.updated_at)

# COMMAND ----------

# MAGIC %md
# MAGIC The output shows `num_updated_rows` and `num_inserted_rows`. Run the MERGE cell **again**: it reports 0 updates, because the same data produces the same result. That is **idempotency**, the most important property of a pipeline step.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, COUNT(*) AS bookings FROM silver_bookings GROUP BY status ORDER BY status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta keeps the audit trail of every pipeline step
# MAGIC SELECT version, timestamp, operation, operationMetrics.numTargetRowsUpdated, operationMetrics.numTargetRowsInserted
# MAGIC FROM (DESCRIBE HISTORY silver_bookings)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Gold: business-level aggregate
# MAGIC Finance's daily revenue table, built from silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_revenue
# MAGIC COMMENT 'Gold: daily bookings, realised revenue and cancellations by destination'
# MAGIC AS
# MAGIC SELECT TO_DATE(b.created_at)                                                    AS booking_date,
# MAGIC        d.destination,
# MAGIC        COUNT(*)                                                                 AS bookings,
# MAGIC        ROUND(SUM(CASE WHEN b.status IN ('confirmed','completed') THEN b.total_amount END), 2) AS realised_revenue,
# MAGIC        COUNT_IF(b.status = 'cancelled')                                         AS cancellations,
# MAGIC        CAST(:run_date AS DATE)                                                  AS pipeline_run_date,
# MAGIC        CURRENT_TIMESTAMP()                                                      AS refreshed_at
# MAGIC FROM silver_bookings b
# MAGIC JOIN samples.wanderbricks.properties   p ON b.property_id = p.property_id
# MAGIC JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC SELECT destination, SUM(realised_revenue) AS revenue FROM gold_daily_revenue GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC > `:run_date` reads the **widget** directly in SQL, so SQL cells are parameterised too.
# MAGIC
# MAGIC ---
# MAGIC ## 6. From notebook to production job (Module 2.1 · Part 2)
# MAGIC
# MAGIC Interactive cells are great for development; production needs **automation**. We split the pipeline into **task notebooks** (in the `day2_job_tasks/` folder next to this notebook):
# MAGIC
# MAGIC | Task key | Notebook | Depends on | Purpose |
# MAGIC |---|---|---|---|
# MAGIC | `ingest_bronze` | `day2_job_tasks/t1_ingest` | (none) | Auto Loader → bronze; publishes `rows_ingested` as a **task value** |
# MAGIC | `merge_silver` | `day2_job_tasks/t2_silver_merge` | `ingest_bronze` | MERGE latest changes → silver |
# MAGIC | `build_gold` | `day2_job_tasks/t3_gold` | `merge_silver` | Rebuild gold revenue |
# MAGIC | `dq_check` | `day2_job_tasks/t4_dq_check` | `merge_silver` | Fail the run if `booking_id` is not unique |
# MAGIC
# MAGIC `build_gold` and `dq_check` run **in parallel** after silver: a task **graph** (DAG), not just a list.
# MAGIC
# MAGIC **Job parameters:** `catalog`, `schema`, `run_date`, passed to every task as widgets.
# MAGIC **Schedule:** daily at 02:00 IST, created **paused** so 60 participants do not all trigger it.
# MAGIC
# MAGIC You can build the same thing in the UI: **Jobs & Pipelines → Create → Job → + Add task**. Here we use the **Databricks SDK** (pre-installed on serverless) so it is repeatable, which is the "jobs as code" practice.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()   # authenticates automatically as you, inside a notebook

this_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
TASKS_DIR = this_notebook.rsplit("/", 1)[0] + "/day2_job_tasks"
JOB_NAME = f"wb_bookings_pipeline_{USER_SCHEMA}"

def nb(name, **params):
    return jobs.NotebookTask(notebook_path=f"{TASKS_DIR}/{name}", base_parameters=params or None)

job_spec = dict(
    name=JOB_NAME,
    description="Day 2 demo: nightly Wanderbricks bookings pipeline (bronze → silver → gold + DQ)",
    tags={"course": "databricks-catalyst", "owner": USER_SCHEMA},
    max_concurrent_runs=1,
    parameters=[
        jobs.JobParameterDefinition(name="catalog",  default=CATALOG),
        jobs.JobParameterDefinition(name="schema",   default=USER_SCHEMA),
        jobs.JobParameterDefinition(name="run_date", default=RUN_DATE),
    ],
    tasks=[
        jobs.Task(task_key="ingest_bronze", notebook_task=nb("t1_ingest")),
        jobs.Task(task_key="merge_silver",  notebook_task=nb("t2_silver_merge"),
                  depends_on=[jobs.TaskDependency(task_key="ingest_bronze")]),
        jobs.Task(task_key="build_gold",    notebook_task=nb("t3_gold"),
                  depends_on=[jobs.TaskDependency(task_key="merge_silver")]),
        jobs.Task(task_key="dq_check",      notebook_task=nb("t4_dq_check", table="silver_bookings", key_cols="booking_id"),
                  depends_on=[jobs.TaskDependency(task_key="merge_silver")]),
    ],
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 2 * * ?", timezone_id="Asia/Kolkata",
                               pause_status=jobs.PauseStatus.PAUSED),
)

# Idempotent: update the job if it already exists, otherwise create it
existing = list(w.jobs.list(name=JOB_NAME))
if existing:
    JOB_ID = existing[0].job_id
    w.jobs.reset(job_id=JOB_ID, new_settings=jobs.JobSettings(**job_spec))
else:
    JOB_ID = w.jobs.create(**job_spec).job_id

print(f"Job '{JOB_NAME}' id={JOB_ID}")
displayHTML(f'<a href="/jobs/{JOB_ID}" target="_blank">Open the job in the Jobs UI</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC **Open the job** and look at the **Tasks** tab: you will see the graph `ingest_bronze → merge_silver → (build_gold ‖ dq_check)`, the **Schedule & triggers** panel (paused), and **Parameters**.
# MAGIC
# MAGIC ---
# MAGIC ## 7. Run the job
# MAGIC
# MAGIC Tonight's file (batch 5) arrives, and we trigger the job with **Run now**. The helper below waits for the run and prints a per-task summary.

# COMMAND ----------

import time

def wait_for_run(run_id, repair_id=None, poll=15):
    """Poll a job run (or one of its repairs) until it finishes; return the Run object."""
    while True:
        r = w.jobs.get_run(run_id, include_history=True)
        state = r.state
        if repair_id is not None:
            state = next(h.state for h in r.repair_history if h.id == repair_id)
        if state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return r
        time.sleep(poll)

def summarize(run):
    rows = [(t.task_key, t.attempt_number, t.state.result_state.value if t.state.result_state else t.state.life_cycle_state.value,
             round((t.execution_duration or 0) / 1000, 1)) for t in run.tasks]
    display(spark.createDataFrame(rows, "task string, attempt int, result string, seconds double"))

print("Landed", drop_batch(5))
run_id = w.jobs.run_now(job_id=JOB_ID).run_id
displayHTML(f'<a href="/jobs/{JOB_ID}/runs/{run_id}" target="_blank">View run {run_id}</a>')
run = wait_for_run(run_id)
print("Run result:", run.state.result_state)
summarize(run)
assert run.state.result_state.value == "SUCCESS", "Job run failed: open the run link above and check the failed task's output"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The job only ingested the new file
# MAGIC SELECT _source_file, COUNT(*) AS rows, MAX(_ingested_at) AS ingested_at FROM bronze_booking_updates GROUP BY ALL ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dq_results ORDER BY checked_at DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Monitoring runs (Module 2.2)
# MAGIC
# MAGIC **Where to look in the UI**
# MAGIC | Screen | What you learn |
# MAGIC |---|---|
# MAGIC | **Jobs & Pipelines → your job → Runs** | Matrix of runs × tasks, coloured by status, with durations: spot slow or flaky tasks |
# MAGIC | **Click a task run** | Notebook output with every cell's result, the **error and stack trace**, task values, parameters used |
# MAGIC | **Compute / Query profile** (serverless) | Query plans, rows processed, time spent per operator |
# MAGIC | **Job settings → Notifications** | Email / Slack / webhook on start, success, failure, or duration over a threshold |
# MAGIC | **System tables** `system.lakeflow.job_run_timeline` | SQL over the run history of *all* jobs (if your admin granted access) |
# MAGIC
# MAGIC The same information is available from the SDK, which is useful for custom monitoring:

# COMMAND ----------

history = [(r.run_id, r.state.result_state.value if r.state.result_state else r.state.life_cycle_state.value,
            r.trigger.value if r.trigger else None, round((r.run_duration or 0) / 1000, 1))
           for r in w.jobs.list_runs(job_id=JOB_ID, limit=10)]
display(spark.createDataFrame(history, "run_id long, result string, trigger string, seconds double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Troubleshooting: break it, read the trace, repair it
# MAGIC
# MAGIC A teammate triggers a backfill with a **typo** in the run date: `2025-13-01`. The following cells show how the job responds.

# COMMAND ----------

bad_run_id = w.jobs.run_now(job_id=JOB_ID, job_parameters={"run_date": "2025-13-01"}).run_id
bad_run = wait_for_run(bad_run_id)
print("Run result:", bad_run.state.result_state)
summarize(bad_run)

# COMMAND ----------

# MAGIC %md
# MAGIC `ingest_bronze` **failed**, and `merge_silver`, `build_gold` and `dq_check` show **UPSTREAM_FAILED**: the job stopped safely instead of loading bad data.
# MAGIC
# MAGIC ### Reading the error
# MAGIC Open the failed task in the UI, or fetch its output programmatically:

# COMMAND ----------

failed_task = next(t for t in bad_run.tasks if t.state.result_state.value == "FAILED")
out = w.jobs.get_run_output(failed_task.run_id)
print("Error:", out.error)
print("\n--- last lines of the stack trace ---")
print("\n".join((out.error_trace or "").strip().splitlines()[-8:]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to read a stack trace
# MAGIC 1. Start at the **bottom**: the last line is the actual exception (`ValueError: … does not match format '%Y-%m-%d'`).
# MAGIC 2. Walk **up** to find the first frame in **your** code (the notebook cell), not library internals.
# MAGIC 3. Spark errors have an **error class** in capitals (for example, `TABLE_OR_VIEW_NOT_FOUND` or `CAST_INVALID_INPUT`). Search for it in the docs or ask the Assistant with **/fix**.
# MAGIC
# MAGIC | Common error class | Usual cause |
# MAGIC |---|---|
# MAGIC | `TABLE_OR_VIEW_NOT_FOUND` | Wrong catalog/schema, or missing `USE` / permission |
# MAGIC | `UNRESOLVED_COLUMN` | Typo or schema change upstream |
# MAGIC | `PERMISSION_DENIED` | Missing `USE SCHEMA` / `SELECT` / `MODIFY` |
# MAGIC | `DELTA_MERGE_MULTIPLE_SOURCE_ROWS_MATCHED` | Source not deduplicated before MERGE |
# MAGIC | `CAST_INVALID_INPUT` | Bad data or parameter type |
# MAGIC
# MAGIC ### Repair the run
# MAGIC **Repair run** re-executes **only the failed and skipped tasks**, optionally with corrected parameters. Successful tasks are not re-run, which saves time and cost on big pipelines.

# COMMAND ----------

repair_id = w.jobs.repair_run(run_id=bad_run_id, rerun_all_failed_tasks=True,
                              job_parameters={"run_date": RUN_DATE}).response.repair_id
repaired = wait_for_run(bad_run_id, repair_id=repair_id)
final_state = next(h.state for h in repaired.repair_history if h.id == repair_id)
print("Repair result:", final_state.result_state)
summarize(repaired)

# COMMAND ----------

# MAGIC %md
# MAGIC In the UI, the run now shows **two attempts**, the original and the repair, under the same run.
# MAGIC
# MAGIC ---
# MAGIC ## Day 2 recap
# MAGIC | You did | Concept |
# MAGIC |---|---|
# MAGIC | Widgets, `%sql`/`%py`/`%md`, temp views, `:param` in SQL | Notebook authoring |
# MAGIC | `/explain`, `/doc`, `/fix` | Databricks Assistant |
# MAGIC | CTAS + `read_files`, `COPY INTO`, Auto Loader | Batch vs incremental ingestion |
# MAGIC | Bronze → silver (MERGE) → gold | Medallion architecture, idempotency |
# MAGIC | SDK-defined job with a DAG, parameters, paused schedule | Workflows / Lakeflow Jobs |
# MAGIC | Run history, error + stack trace, repair run | Monitoring & troubleshooting |
# MAGIC
# MAGIC ### Knowledge check
# MAGIC 1. Which ingestion method would you choose for 2 million new files per day, and why?
# MAGIC 2. What makes the MERGE step idempotent?
# MAGIC 3. Why did `build_gold` not run when `ingest_bronze` failed?
# MAGIC 4. What is the difference between **Run now** again and **Repair run**?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. Auto Loader: scalable file discovery, checkpointing and schema evolution.
# MAGIC 2. Deduplicating the source to the latest change, plus the `s.updated_at > t.updated_at` condition, so applying the same data twice changes nothing.
# MAGIC 3. Task dependencies: downstream tasks are marked `UPSTREAM_FAILED`.
# MAGIC 4. Run now starts a new run of every task; Repair re-runs only failed/skipped tasks in the *same* run.
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC ## Cleanup (optional)
# MAGIC Deletes the job, the landing volume and the Day 2 tables. **Keep them if you are doing 02B or 03A** (03A reads `silver_bookings`).

# COMMAND ----------

dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"], "Drop objects created here?")
if dbutils.widgets.get("cleanup") == "yes":
    for j in w.jobs.list(name=JOB_NAME):
        w.jobs.delete(j.job_id)
        print("Deleted job:", j.job_id)
    for stmt in ["DROP TABLE IF EXISTS bronze_bu_ctas", "DROP TABLE IF EXISTS bronze_bu_copy_into",
                 "DROP TABLE IF EXISTS bronze_booking_updates", "DROP TABLE IF EXISTS silver_bookings",
                 "DROP TABLE IF EXISTS gold_daily_revenue", "DROP TABLE IF EXISTS stg_booking_update_batches",
                 "DROP TABLE IF EXISTS dq_results", "DROP VOLUME IF EXISTS landing"]:
        spark.sql(stmt)
        print("Executed:", stmt)
else:
    print("Cleanup skipped (set widget 'cleanup' = yes to drop Day 2 objects and the job)")