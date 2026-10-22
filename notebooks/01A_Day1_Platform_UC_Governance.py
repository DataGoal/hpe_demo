# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01A · Day 1: Explore the Platform, Unity Catalog & Access Control
# MAGIC ### Databricks Catalyst · Instructor-led demo
# MAGIC
# MAGIC | Module | Topic | Sections |
# MAGIC |---|---|---|
# MAGIC | 1.1 | Databricks & Lakehouse fundamentals | §1–§2 |
# MAGIC | 1.2 | Data discovery with Unity Catalog | §3–§5 |
# MAGIC | 1.3 | Access control & secret scopes | §6–§8 |
# MAGIC
# MAGIC ### Learning objectives
# MAGIC - Explain the **lakehouse** and why Delta tables give warehouse reliability on cheap lake storage
# MAGIC - Choose the right **compute** (serverless, classic all-purpose, job compute, SQL warehouse)
# MAGIC - Navigate the **3-level namespace** `catalog.schema.object` and discover data with SQL and the UI
# MAGIC - Create governed objects: **managed tables, views, volumes, functions**, with comments
# MAGIC - **GRANT / REVOKE** privileges and protect PII with a **dynamic view**
# MAGIC - Use **secret scopes** instead of hard-coded credentials
# MAGIC
# MAGIC ### Today's scenario
# MAGIC > **Request from Wanderbricks Finance:** *"We need a monthly revenue view by destination. Per Security policy, guest email addresses must not be visible to our team. Please locate the appropriate data, build the dataset in a governed manner, and share only what is required."*
# MAGIC >
# MAGIC > As the data engineer assigned to this request, you will complete it in the sections below.

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
# MAGIC ## 1. Lakehouse fundamentals (Module 1.1)
# MAGIC
# MAGIC | | Data warehouse | Data lake | **Lakehouse** |
# MAGIC |---|---|---|---|
# MAGIC | Data types | Structured only | Anything (files) | **Anything** |
# MAGIC | Format | Proprietary | Open (Parquet, JSON…) | **Open table formats** (Delta Lake / Iceberg) |
# MAGIC | Reliability | ACID transactions | No transactions (a "data swamp" risk) | **ACID on the lake** |
# MAGIC | Workloads | BI | DS / ML / streaming | **BI + ETL + streaming + ML/AI on one copy** |
# MAGIC | Governance | Table ACLs | File/blob permissions | **Unity Catalog for everything** |
# MAGIC
# MAGIC **Key idea:** storage (cloud object storage) and compute (clusters / serverless) are **separate**. Data sits once in an open format, and many engines and workloads use it.
# MAGIC
# MAGIC The following cell confirms that `samples.wanderbricks.bookings` is a **Delta table**: Parquet files plus a transaction log in cloud storage.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL samples.wanderbricks.bookings

# COMMAND ----------

# MAGIC %md
# MAGIC **Review:** `format = delta`, `location` (a cloud storage path), `numFiles` and `sizeInBytes`. The table is files in object storage; the compute you are running right now only *reads* them.
# MAGIC
# MAGIC The **transaction log** gives every Delta table a version history (the basis for ACID and **time travel**):

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY samples.wanderbricks.bookings LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Compute: choosing the right engine (Module 1.1)
# MAGIC
# MAGIC | Compute | Best for | Start-up | Who manages it | Billing |
# MAGIC |---|---|---|---|---|
# MAGIC | **Serverless (notebooks & jobs)** ← *what we use* | Interactive notebooks, jobs, quick start | Seconds | Databricks | Per use, no idle cost |
# MAGIC | **All-purpose (classic) cluster** | Custom libraries, init scripts, GPUs, specific Spark config | 3–7 min | You (size, autoscale, auto-terminate) | While running |
# MAGIC | **Job compute** | Production scheduled jobs on classic | Per run | You (defined in job) | Only during the run, cheaper |
# MAGIC | **SQL warehouse** (serverless / pro / classic) | BI, SQL editor, dashboards, Power BI | Seconds (serverless) | Databricks | Per use |
# MAGIC
# MAGIC **Sizing rules of thumb (classic):**
# MAGIC - Start small, enable **autoscaling** (for example, 2–8 workers) and **auto-termination** (for example, 30 min).
# MAGIC - Memory-heavy joins and aggregations → memory-optimised nodes; ML → GPU; heavy shuffles → more cores.
# MAGIC - **Access mode:** *Standard* (shared, UC-governed) for most teams; *Dedicated* (single user) for ML/R/GPU.
# MAGIC
# MAGIC **UI walkthrough:** Sidebar → **Compute** → see *All-purpose*, *Job compute*, *SQL warehouses*. Open the notebook's compute selector (top right) → **Serverless**.
# MAGIC
# MAGIC Below, we ask the engine which version it runs. On serverless you do not choose a runtime version, because Databricks keeps it current.

# COMMAND ----------

print("Spark version :", spark.version)
display(spark.sql("SELECT current_version() AS engine_version, current_user() AS me"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Unity Catalog: the 3-level namespace (Module 1.2)
# MAGIC
# MAGIC ```
# MAGIC Metastore (one per region)
# MAGIC  └── Catalog            e.g. samples, demo-catalog
# MAGIC       └── Schema        e.g. wanderbricks, <your_username>
# MAGIC            └── Table / View / Volume / Function / Model
# MAGIC ```
# MAGIC Every object has a full name: **`catalog.schema.object`**, for example `samples.wanderbricks.bookings`.
# MAGIC Names containing special characters (like `demo-catalog`) must be wrapped in **back-ticks**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN samples

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN samples.wanderbricks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discovering data
# MAGIC User needs **revenue by destination**. Which tables contain money, and which contain *personal data* we must protect?
# MAGIC The `information_schema` lets us **search metadata with SQL**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Where is the money? Where is the PII?
# MAGIC SELECT table_name, column_name, data_type,
# MAGIC        CASE WHEN column_name RLIKE 'email|phone|name' AND table_name IN ('users','hosts','employees') THEN 'PII'
# MAGIC             WHEN column_name RLIKE 'amount|price'                                                   THEN 'Financial'
# MAGIC        END AS tag
# MAGIC FROM samples.information_schema.columns
# MAGIC WHERE table_schema = 'wanderbricks'
# MAGIC   AND column_name RLIKE 'amount|price|email|phone|name'
# MAGIC ORDER BY tag, table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect a table: columns, owner, type (MANAGED/EXTERNAL), location, comment, properties
# MAGIC DESCRIBE TABLE EXTENDED samples.wanderbricks.bookings

# COMMAND ----------

# MAGIC %md
# MAGIC **UI walkthrough: Catalog Explorer** (sidebar → **Catalog**)
# MAGIC 1. **Search** "bookings" in the top bar.
# MAGIC 2. **Browse** `samples` → `wanderbricks` → `bookings`.
# MAGIC 3. **Inspect** the tabs: *Columns*, *Sample Data*, *Details*, *Permissions*, *History*, *Lineage*, *Insights*.
# MAGIC 4. **Trust:** the *Insights* tab shows frequent users and queries; *Lineage* shows upstream and downstream objects.
# MAGIC
# MAGIC ---
# MAGIC ## 4. Build governed objects in your schema (Module 1.2)
# MAGIC
# MAGIC ### 4a. Managed table (CTAS)
# MAGIC We join bookings with properties and destinations to create an enriched, analysis-ready table.
# MAGIC `CREATE OR REPLACE TABLE … AS SELECT` (**CTAS**) creates a **managed Delta table**: Unity Catalog manages both the metadata and the files.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bookings_enriched
# MAGIC COMMENT 'Bookings joined with property and destination attributes. Built in Day 1 demo.'
# MAGIC AS
# MAGIC SELECT b.booking_id,
# MAGIC        b.user_id,
# MAGIC        b.property_id,
# MAGIC        p.property_type,
# MAGIC        d.destination,
# MAGIC        d.country                       AS destination_country,
# MAGIC        b.check_in,
# MAGIC        b.check_out,
# MAGIC        DATEDIFF(b.check_out, b.check_in) AS nights,
# MAGIC        b.guests_count,
# MAGIC        CAST(b.total_amount AS DECIMAL(12,2)) AS total_amount,
# MAGIC        b.status,
# MAGIC        b.created_at
# MAGIC FROM samples.wanderbricks.bookings     b
# MAGIC JOIN samples.wanderbricks.properties   p ON b.property_id = p.property_id
# MAGIC JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id;
# MAGIC
# MAGIC SELECT * FROM bookings_enriched LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Document it: comments make data discoverable
# MAGIC Column comments appear in Catalog Explorer, in search results, and are used by **Genie** (Day 3).

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bookings_enriched ALTER COLUMN total_amount COMMENT 'Booking value in USD';
# MAGIC ALTER TABLE bookings_enriched ALTER COLUMN status       COMMENT 'pending | confirmed | completed | cancelled';
# MAGIC ALTER TABLE bookings_enriched ALTER COLUMN nights       COMMENT 'Length of stay = check_out - check_in (days)';
# MAGIC DESCRIBE TABLE bookings_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. View: share logic, not copies
# MAGIC User only needs **realised revenue** (confirmed + completed) by month and destination. A **view** stores the *query*, not the data, so user always sees fresh numbers.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_bookings_finance
# MAGIC COMMENT 'Finance: monthly realised revenue by destination (no guest PII)'
# MAGIC AS
# MAGIC SELECT DATE_TRUNC('MONTH', check_in) AS stay_month,
# MAGIC        destination,
# MAGIC        destination_country,
# MAGIC        COUNT(*)                      AS bookings,
# MAGIC        SUM(nights)                   AS room_nights,
# MAGIC        SUM(total_amount)             AS revenue_usd
# MAGIC FROM bookings_enriched
# MAGIC WHERE status IN ('confirmed', 'completed')
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC SELECT * FROM v_bookings_finance ORDER BY stay_month DESC, revenue_usd DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d. Function: reusable, governed logic
# MAGIC A SQL **user-defined function (UDF)** lives in the schema and is governed like a table. This one masks an email address, and we will use it for PII protection in §7.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION fn_mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'Masks an email: jane.doe@example.com -> ja***@example.com'
# MAGIC RETURN CASE WHEN email IS NULL THEN NULL
# MAGIC             ELSE CONCAT(LEFT(email, 2), '***@', SPLIT_PART(email, '@', 2)) END;
# MAGIC
# MAGIC SELECT fn_mask_email('priya.sharma@wanderbricks.com') AS masked;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4e. Volume: governed storage for *files*
# MAGIC Tables hold rows; **volumes** hold files (CSV, JSON, images, PDFs). They use the same namespace and the same permissions.
# MAGIC Path pattern: `/Volumes/<catalog>/<schema>/<volume>/…`
# MAGIC
# MAGIC Scenario: the partner team wants a **CSV extract of destinations**. We write it to a volume and read it back.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS raw_files COMMENT 'Landing zone for files used in the Catalyst demos'

# COMMAND ----------

vol = f"/Volumes/{CATALOG}/{USER_SCHEMA}/raw_files"

(spark.table(f"{SRC}.destinations")
      .select("destination_id", "destination", "country", "state_or_province")
      .coalesce(1)
      .write.mode("overwrite").option("header", True)
      .csv(f"{vol}/destinations_csv"))

display(dbutils.fs.ls(f"{vol}/destinations_csv"))

# COMMAND ----------

# read_files() reads files directly from a volume path, the same function used for ingestion on Day 2
display(spark.sql(f"""
  SELECT * FROM read_files('{vol}/destinations_csv', format => 'csv', header => true)
  LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4f. See everything in your schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW USER FUNCTIONS LIKE 'fn_*';

# COMMAND ----------

# MAGIC %md
# MAGIC **Lineage:** open **Catalog → demo-catalog → *your schema* → v_bookings_finance → Lineage**. Unity Catalog captured, automatically, that the view reads `bookings_enriched`, which in turn reads three `samples.wanderbricks` tables, down to column level.
# MAGIC
# MAGIC ---
# MAGIC ## 5. Quick check: managed vs external
# MAGIC | | Managed table (default, recommended) | External table |
# MAGIC |---|---|---|
# MAGIC | Files stored in | UC-managed storage | A path **you** specify (an *external location*) |
# MAGIC | `DROP TABLE` | Deletes metadata **and** files (recoverable for 7 days with `UNDROP`) | Deletes metadata only; files stay |
# MAGIC | Optimisation | Predictive optimisation, liquid clustering automatic | Manual |

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED bookings_enriched

# COMMAND ----------

# MAGIC %md
# MAGIC Look for **Type: MANAGED** in the output above.
# MAGIC
# MAGIC ---
# MAGIC ## 6. Access control: GRANT & REVOKE (Module 1.3)
# MAGIC
# MAGIC ### Privilege model
# MAGIC - To read `catalog.schema.table`, a user needs **`USE CATALOG`** on the catalog, **`USE SCHEMA`** on the schema, **and `SELECT`** on the table/view.
# MAGIC - Privileges **inherit downward**: `SELECT` granted on a *schema* applies to all current and future tables in it.
# MAGIC - The **owner** of an object (you, for objects in your schema) can grant on it.
# MAGIC - Grant to **groups**, not individuals. `account users` is the built-in group of everyone in the account.
# MAGIC
# MAGIC | Privilege | Lets you… |
# MAGIC |---|---|
# MAGIC | `USE CATALOG` / `USE SCHEMA` | "Enter" the container (no data access by itself) |
# MAGIC | `SELECT` / `MODIFY` | Read / write table data |
# MAGIC | `CREATE TABLE` / `CREATE VOLUME` / `CREATE FUNCTION` | Create objects in a schema |
# MAGIC | `READ VOLUME` / `WRITE VOLUME` | Read / write files in a volume |
# MAGIC | `EXECUTE` | Call a function |
# MAGIC | `ALL PRIVILEGES` | Everything above (except ownership) |
# MAGIC
# MAGIC **Scenario:** share *only* the finance view with everyone, not the underlying table.

# COMMAND ----------

# Schema-level statements need the schema name, so we build them in Python (FQ = `demo-catalog`.`<you>`)
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {FQ} TO `account users`")
spark.sql(f"GRANT SELECT ON VIEW {FQ}.v_bookings_finance TO `account users`")
display(spark.sql(f"SHOW GRANTS ON VIEW {FQ}.v_bookings_finance"))

# COMMAND ----------

# MAGIC %md
# MAGIC Any user in the account can now run `SELECT * FROM <your schema>.v_bookings_finance`, but **cannot** read `bookings_enriched`, because they have no `SELECT` on it. The view's owner (you) supplies the access to the underlying table.
# MAGIC
# MAGIC **Optional exercise:** obtain a colleague's schema name and query their `v_bookings_finance`, then their `bookings_enriched`. The second query fails with `PERMISSION_DENIED`.
# MAGIC
# MAGIC Now **revoke**. Least privilege means removing access as soon as it is no longer needed.

# COMMAND ----------

spark.sql(f"REVOKE SELECT ON VIEW {FQ}.v_bookings_finance FROM `account users`")
spark.sql(f"REVOKE USE SCHEMA ON SCHEMA {FQ} FROM `account users`")
display(spark.sql(f"SHOW GRANTS ON VIEW {FQ}.v_bookings_finance"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Protecting PII with a dynamic view (Module 1.3)
# MAGIC
# MAGIC Security's rule: *"Only members of the `pii_readers` group may see real guest emails. Everyone else sees a masked value."*
# MAGIC
# MAGIC A **dynamic view** evaluates the **current user's identity** at query time:
# MAGIC - `current_user()` returns who is querying
# MAGIC - `is_account_group_member('group')` returns whether they belong to a group

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_users_masked
# MAGIC COMMENT 'Users with email masked unless the reader is in pii_readers'
# MAGIC AS
# MAGIC SELECT user_id,
# MAGIC        name,
# MAGIC        CASE WHEN is_account_group_member('pii_readers') THEN email
# MAGIC             ELSE fn_mask_email(email) END AS email,
# MAGIC        country,
# MAGIC        user_type
# MAGIC FROM samples.wanderbricks.users;
# MAGIC
# MAGIC SELECT * FROM v_users_masked LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC You are not in `pii_readers`, so emails come back masked.
# MAGIC In notebook 01B you will apply the same protection with **column masks** and **row filters**, which are attached *directly to a table*, so every query is protected.
# MAGIC
# MAGIC ---
# MAGIC ## 8. Secret scopes: never hard-code credentials (Module 1.3)
# MAGIC
# MAGIC **Never** do this in a notebook:
# MAGIC ```python
# MAGIC jdbc_password = "Sup3rS3cret!"        # visible to anyone who can read the notebook, its revisions and git history
# MAGIC ```
# MAGIC
# MAGIC Store the value in a **secret scope** and read it at run time:
# MAGIC ```bash
# MAGIC # One-time setup by an admin (Databricks CLI)
# MAGIC databricks secrets create-scope training
# MAGIC databricks secrets put-secret training partner_db_password
# MAGIC databricks secrets put-acl training "account users" READ
# MAGIC ```
# MAGIC ```python
# MAGIC pwd = dbutils.secrets.get(scope="training", key="partner_db_password")
# MAGIC ```
# MAGIC ```sql
# MAGIC -- also from SQL
# MAGIC SELECT secret('training', 'partner_db_password')
# MAGIC ```
# MAGIC Secret values are **redacted** in notebook output (`[REDACTED]`), even if you try to `print` them.
# MAGIC
# MAGIC The next cell is **safe to run** whether or not a scope exists in this workspace.

# COMMAND ----------

try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    print("Secret scopes visible to you:", scopes or "none")
    if "training" in scopes:
        value = dbutils.secrets.get(scope="training", key="partner_db_password")
        print("Retrieved secret ->", value)          # prints [REDACTED]
    else:
        print("No 'training' scope here: in production an admin creates it (see the CLI snippet above).")
except Exception as e:
    print(f"Secrets API not available to this user ({type(e).__name__}). Concept shown above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Day 1 recap
# MAGIC | You did | Concept |
# MAGIC |---|---|
# MAGIC | `DESCRIBE DETAIL` / `HISTORY` on a sample table | Delta = open files + transaction log, separate from compute |
# MAGIC | Picked serverless compute | Compute options and sizing |
# MAGIC | `SHOW` / `DESCRIBE` / `information_schema` / Catalog Explorer | Discovery in the 3-level namespace |
# MAGIC | Created a table, view, function and volume | UC securable objects |
# MAGIC | `GRANT` / `REVOKE` on a view | Least-privilege sharing |
# MAGIC | Dynamic view with `is_account_group_member` | PII protection |
# MAGIC | `dbutils.secrets` | Credentials outside code |
# MAGIC
# MAGIC ### Knowledge check (discussion)
# MAGIC 1. You granted `SELECT` on a view but your colleague still gets `PERMISSION_DENIED`. What is missing?
# MAGIC 2. Why is a view safer than copying data into a new table for Finance?
# MAGIC 3. What happens to the files when you `DROP` a managed table, and what about an external one?
# MAGIC 4. A job needs a database password. Where should it live?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. `USE CATALOG` on `demo-catalog` and/or `USE SCHEMA` on your schema.
# MAGIC 2. There is one copy of the data, it is always fresh, and you control exactly which columns and rows are exposed.
# MAGIC 3. Managed: the files are deleted (recoverable with `UNDROP` within 7 days). External: only the metadata is removed.
# MAGIC 4. In a secret scope, read with `dbutils.secrets.get` / `secret()`.
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC ## Cleanup (optional)
# MAGIC Keep these objects if you plan to do **01B**, because it reuses `bookings_enriched` and `fn_mask_email`.

# COMMAND ----------

dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"], "Drop objects created here?")
if dbutils.widgets.get("cleanup") == "yes":
    for stmt in ["DROP VIEW IF EXISTS v_users_masked",
                 "DROP VIEW IF EXISTS v_bookings_finance",
                 "DROP TABLE IF EXISTS bookings_enriched",
                 "DROP FUNCTION IF EXISTS fn_mask_email",
                 "DROP VOLUME IF EXISTS raw_files"]:
        spark.sql(stmt)
        print("Executed:", stmt)
else:
    print("Cleanup skipped (set widget 'cleanup' = yes to drop Day 1 objects)")