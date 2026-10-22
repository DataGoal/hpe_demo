# Databricks notebook source
# MAGIC %md
# MAGIC # 01B · Day 1: Deep-Dive Exercises (self-led)
# MAGIC ### Databricks Catalyst · Unity Catalog, governance & Delta
# MAGIC
# MAGIC **Time:** 60–90 min · **Prerequisite:** run notebook **01A** (or at least its setup cell)
# MAGIC
# MAGIC ### Scenario: the Host Directory
# MAGIC > **Trust & Safety** wants a *Host Directory* table: one row per host with portfolio size, staff count and average nightly price.
# MAGIC > **Legal** has two rules:
# MAGIC > 1. Contact details (email, phone) must be **masked** for everyone except the `pii_readers` group.
# MAGIC > 2. The India operations team may see **only Indian hosts**.
# MAGIC >
# MAGIC >
# MAGIC > In addition, a recent accidental deletion must be recovered without restoring from an external backup.
# MAGIC
# MAGIC ### How to use this notebook
# MAGIC Each task has:
# MAGIC - **Task**: the business requirement
# MAGIC - **Hint**: syntax guidance (expand only if needed)
# MAGIC - **Expected outcome**: the result to verify against
# MAGIC - A **TODO cell** for your code, followed by a **check cell** that prints PASS or FAIL
# MAGIC
# MAGIC **Solutions** are provided at the end of the notebook. Attempt each task before reviewing them. You can write SQL in a TODO cell by starting it with `%sql`.

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

# Helper used by every check cell: prints PASS / FAIL / PENDING instead of failing the notebook
CHECK_RESULTS = {}
def check(label, fn):
    try:
        ok, detail = fn()
        CHECK_RESULTS[label] = bool(ok)
        print(("[PASS] " if ok else "[FAIL] ") + label + (f": {detail}" if detail else ""))
    except Exception as e:
        CHECK_RESULTS[label] = False
        print(f"[PENDING] {label}: not completed yet ({type(e).__name__}: {str(e)[:120]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 1 · Discovery: who references hosts?
# MAGIC Before building anything, find **every table in `samples.wanderbricks` that has a `host_id` column**, using `information_schema` (no UI).
# MAGIC Save the result as a view **`v_host_tables`** with a single column `table_name`.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC `samples.information_schema.columns` has `table_schema`, `table_name`, `column_name`. Use `CREATE OR REPLACE VIEW … AS SELECT DISTINCT …`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 3 tables: `employees`, `hosts`, `properties`.

# COMMAND ----------

# TODO Task 1

# COMMAND ----------

check("Task 1 · v_host_tables", lambda: (
    sorted(r[0] for r in spark.table("v_host_tables").collect()) == ["employees", "hosts", "properties"],
    f"{spark.table('v_host_tables').count()} tables found"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 2 · Build the Host Directory (managed table)
# MAGIC Create managed table **`hosts_directory`**, one row per host, with:
# MAGIC
# MAGIC | Column | Logic |
# MAGIC |---|---|
# MAGIC | `host_id`, `name`, `email`, `phone`, `country`, `is_verified`, `is_active`, `rating` | from `hosts` |
# MAGIC | `property_count` | number of properties the host lists (0 if none) |
# MAGIC | `avg_base_price` | average `base_price` of their properties, rounded to 2 decimals |
# MAGIC | `active_staff` | employees with `is_currently_employed = true` **excluding** `role = 'owner'` |
# MAGIC
# MAGIC Then add a **table comment** and a **column comment** on `property_count`.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC - Aggregate `properties` and `employees` in two sub-queries / CTEs **before** joining to `hosts`, so you avoid a fan-out that multiplies counts.
# MAGIC - `LEFT JOIN` + `COALESCE(x, 0)` keeps hosts with no properties.
# MAGIC - `COMMENT ON TABLE t IS '…'` and `ALTER TABLE t ALTER COLUMN c COMMENT '…'`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 19,384 rows (one per host); the sum of `property_count` = 18,163 (every property).

# COMMAND ----------

# TODO Task 2

# COMMAND ----------

check("Task 2 · hosts_directory", lambda: (
    spark.sql("SELECT COUNT(*) = 19384 AND SUM(property_count) = 18163 FROM hosts_directory").first()[0]
    and spark.sql("DESCRIBE TABLE EXTENDED hosts_directory").filter("col_name = 'Comment'").count() == 1,
    spark.sql("SELECT CONCAT(COUNT(*), ' rows, ', SUM(property_count), ' properties') FROM hosts_directory").first()[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 3 · Recover from an accidental delete with Delta time travel
# MAGIC Run the simulated-incident cell below, which **deletes every host rated below 3**. Then:
# MAGIC 1. Use `DESCRIBE HISTORY` to find the **version before the DELETE**.
# MAGIC 2. Query that version with `VERSION AS OF` to prove the data is still there.
# MAGIC 3. **`RESTORE`** the table to that version.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY hosts_directory;
# MAGIC SELECT COUNT(*) FROM hosts_directory VERSION AS OF <n>;
# MAGIC RESTORE TABLE hosts_directory TO VERSION AS OF <n>;
# MAGIC ```
# MAGIC In Python you can find `<n>` with `spark.sql("DESCRIBE HISTORY hosts_directory").filter("operation = 'DELETE'")`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** after restore, `hosts_directory` has 19,384 rows again, and `DESCRIBE HISTORY` shows a `RESTORE` operation.

# COMMAND ----------

# Simulated incident: run this cell once, after completing Task 2
if spark.catalog.tableExists("hosts_directory"):
    deleted = spark.sql("DELETE FROM hosts_directory WHERE rating < 3").first()[0]
    print(f"Simulated incident: deleted {deleted} hosts")
else:
    print("Do Task 2 first")

# COMMAND ----------

# TODO Task 3

# COMMAND ----------

check("Task 3 · restored", lambda: (
    spark.table("hosts_directory").count() == 19384
    and spark.sql("DESCRIBE HISTORY hosts_directory").filter("operation = 'RESTORE'").count() > 0,
    f"{spark.table('hosts_directory').count()} rows now"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 4 · Column masks for PII
# MAGIC Legal needs a **secure copy** that anyone can query safely.
# MAGIC 1. Create table **`hosts_directory_secure`** as a copy of `hosts_directory`.
# MAGIC 2. Create SQL function **`fn_mask_contact(val STRING) RETURNS STRING`** that returns `val` if the user is in account group `pii_readers`, and otherwise `'***'` followed by the last 4 characters (for example, `***4268`).
# MAGIC 3. Attach it as a **column mask** to both `email` and `phone`.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE t ALTER COLUMN email SET MASK fn_mask_contact;
# MAGIC ```
# MAGIC `is_account_group_member('pii_readers')`, `RIGHT(val, 4)`. Unlike the dynamic view in 01A, a mask lives **on the table**, so *every* query path is protected.
# MAGIC If you re-run, `DROP TABLE IF EXISTS hosts_directory_secure` first.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** `SELECT email, phone FROM hosts_directory_secure` returns values starting with `***`.

# COMMAND ----------

# TODO Task 4

# COMMAND ----------

check("Task 4 · column masks", lambda: (
    spark.sql("SELECT COUNT_IF(email NOT LIKE '***%') + COUNT_IF(phone NOT LIKE '***%') FROM hosts_directory_secure").first()[0] == 0,
    spark.sql("SELECT CONCAT(email, ' | ', phone) FROM hosts_directory_secure LIMIT 1").first()[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 5 · Row filter for regional access
# MAGIC Create function **`fn_host_region(country STRING) RETURNS BOOLEAN`** that returns `TRUE` when the user is in group `catalyst_admins` **or** the row's country is `'India'`.
# MAGIC Attach it as a **row filter** on `hosts_directory_secure` (column `country`).
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE t SET ROW FILTER fn_host_region ON (country);
# MAGIC ```
# MAGIC Row filters apply to the **owner too**, so you will now see fewer rows yourself.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** `SELECT COUNT(*) FROM hosts_directory_secure` = **7,149** (Indian hosts only), assuming nothing was deleted, because the copy was taken *after* the restore.

# COMMAND ----------

# TODO Task 5

# COMMAND ----------

check("Task 5 · row filter", lambda: (
    spark.table("hosts_directory_secure").count() == 7149,
    f"{spark.table('hosts_directory_secure').count()} rows visible"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 6 · Grant, verify, revoke
# MAGIC Give everyone (`account users`) read access to **`hosts_directory_secure` only** (not `hosts_directory`). Show the grants, save them to a table **`grant_audit`** for your records, then **revoke** them.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC Access needs `USE SCHEMA` on your schema **and** `SELECT` on the table. Schema-level statements need the full name, so use the Python variable `FQ` in an f-string: `spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FQ} TO `account users`")`.
# MAGIC `spark.sql("SHOW GRANTS ON TABLE …")` returns a DataFrame that you can `.write.saveAsTable()`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** `grant_audit` contains a row with principal `account users` and action `SELECT`; after the revoke, `SHOW GRANTS` no longer lists it.

# COMMAND ----------

# TODO Task 6

# COMMAND ----------

check("Task 6 · grant audit + revoke", lambda: (
    spark.sql("SELECT COUNT(*) FROM grant_audit WHERE Principal = 'account users' AND ActionType = 'SELECT'").first()[0] >= 1
    and spark.sql(f"SHOW GRANTS ON TABLE {FQ}.hosts_directory_secure").filter("Principal = 'account users'").count() == 0,
    "granted then revoked"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 7 · Clone, drop and UNDROP
# MAGIC Before a risky change, the team makes a backup.
# MAGIC 1. Create **`hosts_backup`** as a **DEEP CLONE** of `hosts_directory`.
# MAGIC 2. Inspect it with `DESCRIBE DETAIL`: is it managed, and where are its files?
# MAGIC 3. `DROP` it… then bring it back with **`UNDROP`**.
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC `CREATE OR REPLACE TABLE hosts_backup DEEP CLONE hosts_directory;` then `DROP TABLE hosts_backup;` then `UNDROP TABLE hosts_backup;`. UNDROP works for **managed** tables within 7 days.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** `hosts_backup` exists with 19,384 rows after `UNDROP`.

# COMMAND ----------

# TODO Task 7

# COMMAND ----------

check("Task 7 · undrop", lambda: (spark.table("hosts_backup").count() == 19384, "hosts_backup is back"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Reflection questions
# MAGIC 1. In Task 2, what would go wrong if you joined `hosts → properties → employees` in one step and then counted?
# MAGIC 2. Column mask vs dynamic view: when would you choose each?
# MAGIC 3. Why did the row filter also hide rows from **you**, the owner?
# MAGIC 4. Time travel lets you go back in time, but for how long, and what controls it?
# MAGIC 5. A colleague has `SELECT` on `hosts_directory_secure` but not `USE SCHEMA`. What happens?
# MAGIC
# MAGIC ---
# MAGIC ---
# MAGIC # Solutions
# MAGIC > **Note:** Attempt the tasks before reviewing this section. The cells below contain complete solutions for comparison.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_host_tables AS
# MAGIC SELECT DISTINCT table_name
# MAGIC FROM samples.information_schema.columns
# MAGIC WHERE table_schema = 'wanderbricks' AND column_name = 'host_id';
# MAGIC
# MAGIC SELECT * FROM v_host_tables ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2
# MAGIC Aggregate first, then join, to avoid a fan-out (one host × many properties × many employees would multiply the counts).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean up any secure copy from a previous run (it depends on this table)
# MAGIC DROP TABLE IF EXISTS hosts_directory_secure;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE hosts_directory
# MAGIC COMMENT 'Trust & Safety host directory: one row per host with portfolio and staffing KPIs'
# MAGIC AS
# MAGIC WITH props AS (
# MAGIC   SELECT host_id, COUNT(*) AS property_count, ROUND(AVG(base_price), 2) AS avg_base_price
# MAGIC   FROM samples.wanderbricks.properties GROUP BY host_id
# MAGIC ), staff AS (
# MAGIC   SELECT host_id, COUNT(*) AS active_staff
# MAGIC   FROM samples.wanderbricks.employees
# MAGIC   WHERE is_currently_employed AND role <> 'owner'
# MAGIC   GROUP BY host_id
# MAGIC )
# MAGIC SELECT h.host_id, h.name, h.email, h.phone, h.country, h.is_verified, h.is_active,
# MAGIC        ROUND(h.rating, 2)               AS rating,
# MAGIC        COALESCE(p.property_count, 0)    AS property_count,
# MAGIC        p.avg_base_price,
# MAGIC        COALESCE(s.active_staff, 0)      AS active_staff
# MAGIC FROM samples.wanderbricks.hosts h
# MAGIC LEFT JOIN props p ON h.host_id = p.host_id
# MAGIC LEFT JOIN staff s ON h.host_id = s.host_id;
# MAGIC
# MAGIC ALTER TABLE hosts_directory ALTER COLUMN property_count COMMENT 'Number of listings owned by the host';
# MAGIC
# MAGIC SELECT COUNT(*) AS hosts, SUM(property_count) AS properties FROM hosts_directory;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3
# MAGIC The first cell below replays the simulated incident so the solution is self-contained; the second restores the table.

# COMMAND ----------

spark.sql("DELETE FROM hosts_directory WHERE rating < 3")   # replay the accident so the solution is self-contained
display(spark.sql("DESCRIBE HISTORY hosts_directory").select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

last_delete = spark.sql("DESCRIBE HISTORY hosts_directory").filter("operation = 'DELETE'").agg({"version": "max"}).first()[0]
good_version = last_delete - 1
print("Rows now                     :", spark.table("hosts_directory").count())
print(f"Rows at version {good_version:<13}:", spark.sql(f"SELECT COUNT(*) FROM hosts_directory VERSION AS OF {good_version}").first()[0])

spark.sql(f"RESTORE TABLE hosts_directory TO VERSION AS OF {good_version}")
print("Rows after RESTORE           :", spark.table("hosts_directory").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS hosts_directory_secure;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION fn_mask_contact(val STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Column mask: full value for pii_readers, otherwise *** + last 4 chars'
# MAGIC RETURN CASE WHEN is_account_group_member('pii_readers') THEN val
# MAGIC             ELSE CONCAT('***', RIGHT(val, 4)) END;
# MAGIC
# MAGIC CREATE TABLE hosts_directory_secure
# MAGIC COMMENT 'Host directory with PII masks and regional row filter'
# MAGIC AS SELECT * FROM hosts_directory;
# MAGIC
# MAGIC ALTER TABLE hosts_directory_secure ALTER COLUMN email SET MASK fn_mask_contact;
# MAGIC ALTER TABLE hosts_directory_secure ALTER COLUMN phone SET MASK fn_mask_contact;
# MAGIC
# MAGIC SELECT host_id, name, email, phone, country FROM hosts_directory_secure LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION fn_host_region(country STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Row filter: admins see all hosts; everyone else sees India only'
# MAGIC RETURN is_account_group_member('catalyst_admins') OR country = 'India';
# MAGIC
# MAGIC ALTER TABLE hosts_directory_secure SET ROW FILTER fn_host_region ON (country);
# MAGIC
# MAGIC SELECT country, COUNT(*) AS hosts FROM hosts_directory_secure GROUP BY country;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FQ} TO `account users`")
spark.sql(f"GRANT SELECT ON TABLE {FQ}.hosts_directory_secure TO `account users`")

grants = spark.sql(f"SHOW GRANTS ON TABLE {FQ}.hosts_directory_secure")
grants.write.mode("overwrite").saveAsTable("grant_audit")
display(grants)

spark.sql(f"REVOKE SELECT ON TABLE {FQ}.hosts_directory_secure FROM `account users`")
spark.sql(f"REVOKE USE SCHEMA ON SCHEMA {FQ} FROM `account users`")
display(spark.sql(f"SHOW GRANTS ON TABLE {FQ}.hosts_directory_secure"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 7

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE hosts_backup DEEP CLONE hosts_directory;
# MAGIC DESCRIBE DETAIL hosts_backup;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE hosts_backup;
# MAGIC UNDROP TABLE hosts_backup;
# MAGIC SELECT COUNT(*) AS rows_after_undrop FROM hosts_backup;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reflection answers
# MAGIC 1. **Fan-out:** joining both child tables at once creates (properties × staff) rows per host, which inflates `COUNT(*)`. Aggregate each child first.
# MAGIC 2. A **column mask** protects the table itself, so every query, view and tool is covered. A **dynamic view** is good when you want a *curated shape* (subset of columns, joins) as the shared interface.
# MAGIC 3. Row filters and masks apply to **all** principals, including the owner. Ownership does not bypass a policy.
# MAGIC 4. Time travel is limited by data retention: `delta.deletedFileRetentionDuration` (default 7 days) and `VACUUM`. The log is kept for 30 days by default.
# MAGIC 5. They get `PERMISSION_DENIED` (USE SCHEMA is required to "enter" the schema).
# MAGIC
# MAGIC ### Re-run all checks

# COMMAND ----------

dbutils.widgets.dropdown("validate", "no", ["no", "yes"], "Fail notebook if a check fails?")
for label, ok in CHECK_RESULTS.items():
    print(("[PASS] " if ok else "[FAIL] ") + label)
check("Task 1 · v_host_tables", lambda: (spark.table("v_host_tables").count() == 3, None))
check("Task 2 · hosts_directory", lambda: (spark.sql("SELECT SUM(property_count) FROM hosts_directory").first()[0] == 18163, None))
check("Task 3 · restored", lambda: (spark.table("hosts_directory").count() == 19384, None))
check("Task 4 · column masks", lambda: (spark.sql("SELECT COUNT_IF(email NOT LIKE '***%') FROM hosts_directory_secure").first()[0] == 0, None))
check("Task 5 · row filter", lambda: (spark.table("hosts_directory_secure").count() == 7149, None))
check("Task 6 · grant audit + revoke", lambda: (spark.table("grant_audit").filter("Principal = 'account users'").count() >= 1, None))
check("Task 7 · undrop", lambda: (spark.table("hosts_backup").count() == 19384, None))
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
    for stmt in ["DROP TABLE IF EXISTS hosts_directory_secure",
                 "DROP TABLE IF EXISTS hosts_directory",
                 "DROP TABLE IF EXISTS hosts_backup",
                 "DROP TABLE IF EXISTS grant_audit",
                 "DROP VIEW IF EXISTS v_host_tables",
                 "DROP FUNCTION IF EXISTS fn_mask_contact",
                 "DROP FUNCTION IF EXISTS fn_host_region"]:
        spark.sql(stmt)
        print("Executed:", stmt)
else:
    print("Cleanup skipped (set widget 'cleanup' = yes to drop Day 1B objects)")
