# Databricks notebook source
# MAGIC %md
# MAGIC # 02B · Day 2: Deep-Dive Exercises (self-led)
# MAGIC ### Databricks Catalyst · Ingestion, data-quality gates, job failures & repair
# MAGIC
# MAGIC **Time:** 60–90 min · **Prerequisite:** notebook **02A**, and the `instructor_led/day_2/day2_job_tasks/` folder (import the whole `notebooks/` folder with its structure intact)
# MAGIC
# MAGIC ### Scenario: payment reconciliation and the 2 AM failure
# MAGIC > **Finance:** *"Booking totals and payments do not add up. We need a reconciliation table that tells us, per booking, whether it is paid, under-paid, over-paid or unpaid."*
# MAGIC >
# MAGIC > **On-call engineer (you):** you build a payments pipeline with a **data-quality gate**. At 2 AM the gate fails… Find out why, fix the data, and **repair** the run.
# MAGIC
# MAGIC ### How to use this notebook
# MAGIC Each task follows the same structure: Task, Hint, Expected outcome, a TODO cell for your code, and a check cell that prints PASS or FAIL.
# MAGIC **Solutions are provided at the end of the notebook.**

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

# Helpers (provided): checks, SDK client, run polling
import posixpath
import time
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
LANDING = f"/Volumes/{CATALOG}/{USER_SCHEMA}/landing"
# Task notebooks live in notebooks/instructor_led/day_2/day2_job_tasks (two levels up from this folder)
THIS_DIR = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 1)[0]
TASKS_DIR = posixpath.normpath(f"{THIS_DIR}/../../instructor_led/day_2/day2_job_tasks")
DQ_JOB_NAME = f"wb_payments_dq_{USER_SCHEMA}"

CHECK_RESULTS = {}
def check(label, fn):
    try:
        ok, detail = fn()
        CHECK_RESULTS[label] = bool(ok)
        print(("[PASS] " if ok else "[FAIL] ") + label + (f": {detail}" if detail else ""))
    except Exception as e:
        CHECK_RESULTS[label] = False
        print(f"[PENDING] {label}: not completed yet ({type(e).__name__}: {str(e)[:120]})")

def wait_for_run(run_id, repair_id=None, poll=15):
    """Poll a job run (or one of its repairs) until it finishes; return the Run object."""
    while True:
        r = w.jobs.get_run(run_id, include_history=True)
        state = r.state if repair_id is None else next(h.state for h in r.repair_history if h.id == repair_id)
        if state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return r, state
        time.sleep(poll)

print("Task notebooks expected in:", TASKS_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Provided: the payments file lands
# MAGIC The payments system exports a **CSV** file. Run this cell to land it in your volume.

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS landing")
tmp = f"{LANDING}/_tmp/payments"
spark.table(f"{SRC}.payments").coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)
part = [f.path for f in dbutils.fs.ls(tmp) if f.name.endswith(".csv")][0]
dbutils.fs.cp(part, f"{LANDING}/payments/payments_export.csv")
dbutils.fs.rm(tmp, recurse=True)
display(dbutils.fs.ls(f"{LANDING}/payments"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 1 · Parameterised bronze ingestion
# MAGIC Create a **widget** `source_format` (default `csv`). Then, using **CTAS + `read_files`** and that widget value, load the files under `{LANDING}/payments` into **`bronze_payments`**.
# MAGIC Requirements:
# MAGIC - `amount` must be `DECIMAL(15,4)` and `payment_date` a `TIMESTAMP` (use **schema hints**)
# MAGIC - add a `_source_file` column (from `_metadata`) and an `_ingested_at` column
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC ```python
# MAGIC fmt = dbutils.widgets.get("source_format")
# MAGIC spark.sql(f"""CREATE OR REPLACE TABLE bronze_payments AS
# MAGIC   SELECT *, _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
# MAGIC   FROM read_files('{LANDING}/payments', format => '{fmt}', header => true,
# MAGIC                   schemaHints => 'payment_id BIGINT, booking_id BIGINT, amount DECIMAL(15,4), payment_date TIMESTAMP')""")
# MAGIC ```
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 49,638 rows; `amount` is `decimal(15,4)`.

# COMMAND ----------

# TODO Task 1

# COMMAND ----------

check("Task 1 · bronze_payments", lambda: (
    spark.table("bronze_payments").count() == 49638
    and dict(spark.table("bronze_payments").dtypes)["amount"] == "decimal(15,4)",
    f"{spark.table('bronze_payments').count()} rows"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 2 · Profile before you trust
# MAGIC Write **one query** returning these four numbers for `bronze_payments`:
# MAGIC `total_rows`, `duplicate_payment_ids` (rows minus distinct ids), `orphan_payments` (no matching booking in `samples.wanderbricks.bookings`), `bookings_with_multiple_payments`.
# MAGIC Save it as view **`v_payments_profile`**.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC Use scalar sub-queries, or CTEs and a `CROSS JOIN`. Use `LEFT ANTI JOIN` for orphans. For multiple payments: `COUNT(*) - COUNT(DISTINCT booking_id)` counts *extra* rows; you want the number of `booking_id`s with `COUNT(*) > 1`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** `total_rows = 49638`, `duplicate_payment_ids = 4465`, `orphan_payments = 8`, `bookings_with_multiple_payments` is about 3,400.

# COMMAND ----------

# TODO Task 2

# COMMAND ----------

check("Task 2 · v_payments_profile", lambda: (
    tuple(spark.table("v_payments_profile").select("total_rows", "duplicate_payment_ids", "orphan_payments").first()) == (49638, 4465, 8),
    str(spark.table("v_payments_profile").first().asDict())))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 3 · A data-quality gate as a job (and watch it fail)
# MAGIC Using the SDK, create (or reset) a job named `DQ_JOB_NAME` with **one task** `dq_check` that runs notebook `{TASKS_DIR}/t4_dq_check`.
# MAGIC Give the job **parameters** `catalog`, `schema`, `run_date`, `table` (= `bronze_payments`) and `key_cols` (= `payment_id`). Run it and wait.
# MAGIC Then fetch the failed task's **error message** with `w.jobs.get_run_output(task_run_id)` and store the run id in a Python variable **`dq_run_id`**.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC Look at the job definition in 02A §6: you need `jobs.JobParameterDefinition`, `jobs.Task(task_key=…, notebook_task=jobs.NotebookTask(notebook_path=…))`, `w.jobs.create(...)` / `w.jobs.reset(...)`, `w.jobs.run_now(job_id).run_id`, and the `wait_for_run` helper.
# MAGIC `run.tasks[0].run_id` is the **task** run id that `get_run_output` needs.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** the run **FAILS** with `ValueError: DQ check FAILED for bronze_payments: 4465 duplicate and 0 null values in key (payment_id)`.

# COMMAND ----------

# TODO Task 3

# COMMAND ----------

check("Task 3 · DQ gate failed as expected", lambda: (
    w.jobs.get_run(dq_run_id).state.result_state.value == "FAILED"
    and "4465 duplicate" in (w.jobs.get_run_output(w.jobs.get_run(dq_run_id).tasks[0].run_id).error or ""),
    f"run {dq_run_id}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 4 · Fix the data: silver_payments
# MAGIC Build **`silver_payments`**, the trustworthy version:
# MAGIC - **one row per `payment_id`**: keep the row with the latest `payment_date` (ties: highest `amount`)
# MAGIC - **drop orphan payments**: keep only rows whose `booking_id` exists in `samples.wanderbricks.bookings`
# MAGIC - keep columns `payment_id, booking_id, amount, payment_method, status, payment_date`
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC `QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_date DESC, amount DESC) = 1` and `LEFT SEMI JOIN samples.wanderbricks.bookings USING (booking_id)`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** `payment_id` is unique; 0 orphans; row count = number of distinct valid `payment_id`s (about 45K).

# COMMAND ----------

# TODO Task 4

# COMMAND ----------

check("Task 4 · silver_payments", lambda: (
    spark.sql("SELECT COUNT(*) = COUNT(DISTINCT payment_id) FROM silver_payments").first()[0]
    and spark.sql(f"SELECT COUNT(*) FROM silver_payments LEFT ANTI JOIN {SRC}.bookings USING (booking_id)").first()[0] == 0,
    f"{spark.table('silver_payments').count()} rows"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 5 · Repair the failed run
# MAGIC Do not start a new run: **repair** `dq_run_id` with the job parameter `table` overridden to `silver_payments`. Wait for the repair to finish.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC ```python
# MAGIC repair_id = w.jobs.repair_run(run_id=dq_run_id, rerun_all_failed_tasks=True, job_parameters={...}).response.repair_id
# MAGIC run, state = wait_for_run(dq_run_id, repair_id=repair_id)
# MAGIC ```
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** the repair finishes **SUCCESS**; `dq_results` shows a failed row for `bronze_payments` followed by a passed row for `silver_payments`.

# COMMAND ----------

# TODO Task 5

# COMMAND ----------

check("Task 5 · repaired", lambda: (
    spark.sql("SELECT COUNT(*) FROM dq_results WHERE table_name = 'silver_payments' AND passed").first()[0] > 0
    and w.jobs.get_run(dq_run_id).state.result_state.value == "SUCCESS",
    "dq_results has a passing silver_payments check"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 6 · The reconciliation Finance asked for
# MAGIC Build **`payment_recon`**, one row per booking in `samples.wanderbricks.bookings`:
# MAGIC
# MAGIC | Column | Logic |
# MAGIC |---|---|
# MAGIC | `booking_id`, `booking_status`, `booking_total` | from bookings (`total_amount` as `DECIMAL(15,2)`) |
# MAGIC | `paid` | sum of `amount` where payment `status = 'completed'` (0 if none) |
# MAGIC | `refunded` | sum of `amount` where `status = 'refunded'` (0 if none) |
# MAGIC | `net_paid` | `paid - refunded` |
# MAGIC | `recon_status` | `'unpaid'` if no payment rows at all; else `'paid'` if `ABS(net_paid - booking_total) <= 0.01`; else `'underpaid'` / `'overpaid'` |
# MAGIC
# MAGIC Use **`silver_payments`**, not the bronze table.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC Aggregate payments per booking first (`SUM(CASE WHEN status = 'completed' THEN amount END)`), then `LEFT JOIN` to bookings. Use `COUNT(*)` of payment rows (or a flag) to detect "no payments at all".
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 72,247 rows (one per booking). Look at the `recon_status` distribution: most `pending`/`cancelled` bookings are `unpaid`.

# COMMAND ----------

# TODO Task 6

# COMMAND ----------

check("Task 6 · payment_recon", lambda: (
    spark.table("payment_recon").count() == 72247
    and spark.table("payment_recon").select("recon_status").distinct().count() >= 3,
    str({r[0]: r[1] for r in spark.sql("SELECT recon_status, COUNT(*) FROM payment_recon GROUP BY 1").collect()})))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 7 · Monitoring report
# MAGIC Your manager wants a table **`job_run_report`** with every run of **your two jobs** (`wb_bookings_pipeline_<you>` from 02A and `DQ_JOB_NAME`): `job_name`, `run_id`, `result`, `duration_seconds`, `start_time`.
# MAGIC **Bonus:** if you have access, query `system.lakeflow.job_run_timeline` for the same information.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC `w.jobs.list(name=…)` → job id; `w.jobs.list_runs(job_id=…)` → runs with `.run_id`, `.state.result_state`, `.run_duration` (ms), `.start_time` (epoch ms). Build a list of tuples and `spark.createDataFrame(...)`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** at least 1 row for the DQ job (likely 1 run with a repair) and several for the pipeline job if you ran 02A.

# COMMAND ----------

# TODO Task 7

# COMMAND ----------

check("Task 7 · job_run_report", lambda: (
    spark.table("job_run_report").filter(f"job_name = '{DQ_JOB_NAME}'").count() >= 1,
    f"{spark.table('job_run_report').count()} runs reported"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Reflection questions
# MAGIC 1. The table `samples.wanderbricks.payments` *declares* `payment_id` as a primary key. Why did that not prevent duplicates?
# MAGIC 2. Why is it better for the DQ task to **raise an exception** than to just print a warning?
# MAGIC 3. In Task 5, what would have happened if you had clicked **Run now** instead of **Repair**, with no parameter change?
# MAGIC 4. Where would you add an alert so the on-call engineer gets paged at 2 AM?
# MAGIC 5. Name two causes of `paid ≠ booking_total` that are *not* bugs.
# MAGIC
# MAGIC ---
# MAGIC ---
# MAGIC # Solutions
# MAGIC > **Note:** Attempt the tasks before reviewing this section.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1

# COMMAND ----------

dbutils.widgets.text("source_format", "csv", "Source file format")
fmt = dbutils.widgets.get("source_format")

spark.sql(f"""
CREATE OR REPLACE TABLE bronze_payments
COMMENT 'Bronze: raw payments export, loaded as-is with audit columns'
AS
SELECT *, _metadata.file_name AS _source_file, current_timestamp() AS _ingested_at
FROM read_files('{LANDING}/payments',
                format => '{fmt}', header => true,
                schemaHints => 'payment_id BIGINT, booking_id BIGINT, amount DECIMAL(15,4), payment_date TIMESTAMP')
""")
display(spark.sql("DESCRIBE TABLE bronze_payments"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_payments_profile AS
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM bronze_payments)                                              AS total_rows,
# MAGIC   (SELECT COUNT(*) - COUNT(DISTINCT payment_id) FROM bronze_payments)                 AS duplicate_payment_ids,
# MAGIC   (SELECT COUNT(*) FROM bronze_payments p
# MAGIC      LEFT ANTI JOIN samples.wanderbricks.bookings b ON p.booking_id = b.booking_id)   AS orphan_payments,
# MAGIC   (SELECT COUNT(*) FROM (SELECT booking_id FROM bronze_payments
# MAGIC                          GROUP BY booking_id HAVING COUNT(*) > 1))                    AS bookings_with_multiple_payments;
# MAGIC
# MAGIC SELECT * FROM v_payments_profile;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3

# COMMAND ----------

job_spec = dict(
    name=DQ_JOB_NAME,
    description="Day 2B: data-quality gate on payments",
    tags={"course": "databricks-catalyst", "owner": USER_SCHEMA},
    max_concurrent_runs=1,
    parameters=[
        jobs.JobParameterDefinition(name="catalog",  default=CATALOG),
        jobs.JobParameterDefinition(name="schema",   default=USER_SCHEMA),
        jobs.JobParameterDefinition(name="run_date", default="2025-07-31"),
        jobs.JobParameterDefinition(name="table",    default="bronze_payments"),
        jobs.JobParameterDefinition(name="key_cols", default="payment_id"),
    ],
    tasks=[jobs.Task(task_key="dq_check", notebook_task=jobs.NotebookTask(notebook_path=f"{TASKS_DIR}/t4_dq_check"))],
)
existing = list(w.jobs.list(name=DQ_JOB_NAME))
if existing:
    DQ_JOB_ID = existing[0].job_id
    w.jobs.reset(job_id=DQ_JOB_ID, new_settings=jobs.JobSettings(**job_spec))
else:
    DQ_JOB_ID = w.jobs.create(**job_spec).job_id

dq_run_id = w.jobs.run_now(job_id=DQ_JOB_ID).run_id
displayHTML(f'<a href="/jobs/{DQ_JOB_ID}/runs/{dq_run_id}" target="_blank">Open run {dq_run_id}</a>')
run, state = wait_for_run(dq_run_id)
print("Result:", state.result_state)

out = w.jobs.get_run_output(run.tasks[0].run_id)
print("Error :", out.error)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_payments
# MAGIC COMMENT 'Silver: one row per payment_id, orphans removed'
# MAGIC AS
# MAGIC SELECT payment_id, booking_id, amount, payment_method, status, payment_date
# MAGIC FROM bronze_payments
# MAGIC LEFT SEMI JOIN samples.wanderbricks.bookings USING (booking_id)
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_date DESC, amount DESC) = 1;
# MAGIC
# MAGIC SELECT COUNT(*) AS rows, COUNT(DISTINCT payment_id) AS distinct_ids FROM silver_payments;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5

# COMMAND ----------

repair_id = w.jobs.repair_run(run_id=dq_run_id, rerun_all_failed_tasks=True,
                              job_parameters={"table": "silver_payments"}).response.repair_id
run, state = wait_for_run(dq_run_id, repair_id=repair_id)
print("Repair result:", state.result_state)
display(spark.sql("SELECT checked_at, table_name, row_count, duplicate_keys, passed FROM dq_results WHERE table_name LIKE '%payments' ORDER BY checked_at"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE payment_recon
# MAGIC COMMENT 'Finance: booking vs payment reconciliation (one row per booking)'
# MAGIC AS
# MAGIC WITH pay AS (
# MAGIC   SELECT booking_id,
# MAGIC          COUNT(*)                                                     AS payment_rows,
# MAGIC          COALESCE(SUM(CASE WHEN status = 'completed' THEN amount END), 0) AS paid,
# MAGIC          COALESCE(SUM(CASE WHEN status = 'refunded'  THEN amount END), 0) AS refunded
# MAGIC   FROM silver_payments
# MAGIC   GROUP BY booking_id
# MAGIC )
# MAGIC SELECT b.booking_id,
# MAGIC        b.status                                    AS booking_status,
# MAGIC        CAST(b.total_amount AS DECIMAL(15,2))       AS booking_total,
# MAGIC        COALESCE(p.paid, 0)                         AS paid,
# MAGIC        COALESCE(p.refunded, 0)                     AS refunded,
# MAGIC        COALESCE(p.paid, 0) - COALESCE(p.refunded, 0) AS net_paid,
# MAGIC        CASE WHEN p.booking_id IS NULL                                                     THEN 'unpaid'
# MAGIC             WHEN ABS(COALESCE(p.paid,0) - COALESCE(p.refunded,0) - CAST(b.total_amount AS DECIMAL(15,2))) <= 0.01 THEN 'paid'
# MAGIC             WHEN COALESCE(p.paid,0) - COALESCE(p.refunded,0) < CAST(b.total_amount AS DECIMAL(15,2)) THEN 'underpaid'
# MAGIC             ELSE 'overpaid' END                    AS recon_status
# MAGIC FROM samples.wanderbricks.bookings b
# MAGIC LEFT JOIN pay p ON b.booking_id = p.booking_id;
# MAGIC
# MAGIC SELECT booking_status, recon_status, COUNT(*) AS bookings
# MAGIC FROM payment_recon GROUP BY ALL ORDER BY booking_status, bookings DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 7

# COMMAND ----------

from datetime import datetime, timezone

rows = []
for name in (f"wb_bookings_pipeline_{USER_SCHEMA}", DQ_JOB_NAME):
    for j in w.jobs.list(name=name):
        for r in w.jobs.list_runs(job_id=j.job_id, limit=25):
            rows.append((name, r.run_id,
                         r.state.result_state.value if r.state.result_state else r.state.life_cycle_state.value,
                         round((r.run_duration or 0) / 1000, 1),
                         datetime.fromtimestamp(r.start_time / 1000, tz=timezone.utc)))
report = spark.createDataFrame(rows, "job_name string, run_id long, result string, duration_seconds double, start_time timestamp")
report.write.mode("overwrite").saveAsTable("job_run_report")
display(spark.table("job_run_report").orderBy(F.desc("start_time")))

# COMMAND ----------

# Bonus: system tables (works only if an admin granted you SELECT on system.lakeflow)
try:
    ids = ",".join(str(j.job_id) for n in (f"wb_bookings_pipeline_{USER_SCHEMA}", DQ_JOB_NAME) for j in w.jobs.list(name=n))
    display(spark.sql(f"""
      SELECT job_id, run_id, result_state, period_start_time, period_end_time
      FROM system.lakeflow.job_run_timeline
      WHERE job_id IN ({ids}) ORDER BY period_start_time DESC LIMIT 20"""))
except Exception as e:
    print(f"System tables not available to you ({type(e).__name__}). Ask your admin, or use the SDK report above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflection answers
# MAGIC 1. Unity Catalog PK/FK constraints are **informational**: they are not enforced on write. You must check data quality yourself (or use `CHECK` constraints, or *expectations* in Lakeflow Declarative Pipelines).
# MAGIC 2. A failed task **stops downstream tasks**, marks the run red, and triggers **failure notifications**. A printed warning is silently ignored.
# MAGIC 3. A new run with the same parameters fails again on `bronze_payments`. Repair re-runs only the failed task in the same run, with the corrected parameter.
# MAGIC 4. Job **Notifications** (email / Slack / PagerDuty webhook) on *Failure*, plus optionally a *duration threshold* alert.
# MAGIC 5. Partial or instalment payments, refunds, currency rounding, failed payment retries, or pending bookings not yet charged.
# MAGIC
# MAGIC ### Re-run all checks

# COMMAND ----------

dbutils.widgets.dropdown("validate", "no", ["no", "yes"], "Fail notebook if a check fails?")
check("Task 1 · bronze_payments", lambda: (spark.table("bronze_payments").count() == 49638, None))
check("Task 2 · v_payments_profile", lambda: (spark.table("v_payments_profile").first()["duplicate_payment_ids"] == 4465, None))
check("Task 3 · DQ gate failed as expected", lambda: (spark.sql("SELECT COUNT(*) FROM dq_results WHERE table_name = 'bronze_payments' AND NOT passed").first()[0] > 0, None))
check("Task 4 · silver_payments", lambda: (spark.sql("SELECT COUNT(*) = COUNT(DISTINCT payment_id) FROM silver_payments").first()[0], None))
check("Task 5 · repaired", lambda: (w.jobs.get_run(dq_run_id).state.result_state.value == "SUCCESS", None))
check("Task 6 · payment_recon", lambda: (spark.table("payment_recon").count() == 72247, None))
check("Task 7 · job_run_report", lambda: (spark.table("job_run_report").count() >= 1, None))
if dbutils.widgets.get("validate") == "yes":
    failed = [k for k, v in CHECK_RESULTS.items() if not v]
    assert not failed, f"Failed checks: {failed}"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup (optional)

# COMMAND ----------

dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"], "Drop objects created here?")
if dbutils.widgets.get("cleanup") == "yes":
    for j in w.jobs.list(name=DQ_JOB_NAME):
        w.jobs.delete(j.job_id)
        print("Deleted job:", j.job_id)
    for stmt in ["DROP TABLE IF EXISTS bronze_payments", "DROP TABLE IF EXISTS silver_payments",
                 "DROP TABLE IF EXISTS payment_recon", "DROP TABLE IF EXISTS job_run_report",
                 "DROP VIEW IF EXISTS v_payments_profile"]:
        spark.sql(stmt)
        print("Executed:", stmt)
    dbutils.fs.rm(f"{LANDING}/payments", recurse=True)
    print("Removed landing/payments files")
else:
    print("Cleanup skipped (set widget 'cleanup' = yes to drop Day 2B objects and the DQ job)")
