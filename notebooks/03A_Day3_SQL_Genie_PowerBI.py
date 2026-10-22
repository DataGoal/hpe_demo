# Databricks notebook source
# MAGIC %md
# MAGIC # 03A · Day 3: Analyze & Visualize Data
# MAGIC ### Databricks Catalyst · Instructor-led demo
# MAGIC
# MAGIC | Module | Topic | Sections |
# MAGIC |---|---|---|
# MAGIC | 3.1 | Databricks SQL & SQL warehouses: SQL editor, serverless vs classic, query history, saved queries | §1–§4 |
# MAGIC | 3.2 | Genie spaces: natural-language analytics, prompt curation, AI-assisted SQL | §5 |
# MAGIC | 3.3 | Power BI connectivity: DirectQuery vs Import | §6 |
# MAGIC
# MAGIC ### 🎯 Learning objectives
# MAGIC - Choose and configure a **SQL warehouse** (serverless vs pro vs classic, size, scaling, auto-stop)
# MAGIC - Model a **gold star schema** that BI tools and Genie understand: keys, constraints, comments
# MAGIC - Write **analytical SQL**: window functions, `QUALIFY`, `ROLLUP`, `PIVOT`
# MAGIC - Define business KPIs once in a **metric view**
# MAGIC - Use **query history** to find slow or expensive queries
# MAGIC - Set up a **Genie space** with good instructions and trusted SQL
# MAGIC - Connect **Power BI** in the right storage mode
# MAGIC
# MAGIC ### 🏢 Today's scenario
# MAGIC > *"The leadership offsite is next week. I want a **revenue & host performance** dashboard in Power BI, and my regional managers should be able to **just ask questions** like 'Which destinations had the most cancellations last quarter?' without filing a ticket."*
# MAGIC > *VP Commercial, Wanderbricks*

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ Setup: your personal sandbox
# MAGIC
# MAGIC Every participant works in an **isolated schema**: `` `demo-catalog`.`<your_username>` ``.
# MAGIC
# MAGIC This cell:
# MAGIC 1. Reads your login with `current_user()` (for example, `jane.doe@company.com`).
# MAGIC 2. Turns the part before `@` into a valid schema name (lower-case; anything that isn't a letter, digit or `_` becomes `_`). For example, `jane_doe`.
# MAGIC 3. Creates the schema **if it doesn't exist**, so the cell is safe to re-run.
# MAGIC 4. Makes it your default catalog and schema, so unqualified names like `bookings_enriched` land in **your** sandbox.
# MAGIC
# MAGIC > Source data is read **only** from `samples.wanderbricks`. Never write to `samples` or to anyone else's schema.

# COMMAND ----------

import re

CATALOG = "demo-catalog"
SRC = "samples.wanderbricks"

USER_EMAIL = spark.sql("SELECT current_user() AS u").first()["u"]
USER_SCHEMA = re.sub(r"[^a-z0-9_]", "_", USER_EMAIL.split("@")[0].lower())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{USER_SCHEMA}` COMMENT 'Personal sandbox for Databricks Catalyst training'")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{USER_SCHEMA}`")

FQ = f"`{CATALOG}`.`{USER_SCHEMA}`"   # fully-qualified sandbox, handy in f-strings
print(f"User        : {USER_EMAIL}")
print(f"Sandbox     : {FQ}")
print(f"Source data : {SRC} (read-only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Databricks SQL & SQL warehouses (Module 3.1)
# MAGIC
# MAGIC A **SQL warehouse** is compute that is optimised for SQL and BI (Photon engine, result caching, high concurrency). Notebooks, the SQL editor, dashboards, Genie and Power BI can all use one.
# MAGIC
# MAGIC | Type | Start-up | Features | When to use |
# MAGIC |---|---|---|---|
# MAGIC | **Serverless** ✅ recommended | ~5 s | Photon, Intelligent Workload Management, Predictive IO, auto-scaling | Almost always: BI, Genie, ad-hoc |
# MAGIC | **Pro** | minutes | Photon, Predictive IO; runs in *your* cloud account | Networking constraints rule out serverless |
# MAGIC | **Classic** | minutes | Photon, basic | Legacy / lowest feature set |
# MAGIC
# MAGIC **Key settings** (*SQL Warehouses → Create*):
# MAGIC | Setting | Meaning | Training-class suggestion |
# MAGIC |---|---|---|
# MAGIC | **Cluster size** (2X-Small … 4X-Large) | Compute per cluster: bigger = faster *individual* queries | Small |
# MAGIC | **Scaling** (min / max clusters) | More clusters = more *concurrent* users | 1 – 3 |
# MAGIC | **Auto stop** | Stop after N idle minutes, so you don't pay when idle | 10 min |
# MAGIC | **Channel** | Current vs Preview features | Current |
# MAGIC
# MAGIC 🖱️ **SQL editor tour** (sidebar → **SQL Editor**)
# MAGIC 1. Pick a **warehouse** (top right) and the catalog/schema (`demo-catalog` → your schema).
# MAGIC 2. Write a query; **Cmd/Ctrl + Enter** runs the statement under the cursor.
# MAGIC 3. **Save** → it becomes a **saved query** (sidebar → *Queries*) that you can share, schedule, or use in a dashboard or alert.
# MAGIC 4. Click the ✨ **Assistant** and ask: *"Show monthly revenue by continent for 2025"*.
# MAGIC 5. Results pane → **+ Visualization** for a quick chart.
# MAGIC
# MAGIC ---
# MAGIC ## 2. Build the gold star schema
# MAGIC BI tools and Genie work best on a clean **star schema**: one **fact** table (events, with measures) surrounded by **dimension** tables (descriptive attributes).
# MAGIC
# MAGIC ```
# MAGIC                 dim_date
# MAGIC                    │ booking_date
# MAGIC dim_user ── user_id ┼── fact_bookings ── property_id ── dim_property
# MAGIC ```
# MAGIC
# MAGIC > In production, `fact_bookings` would read `silver_bookings` from yesterday's pipeline. Here we read the sample snapshot so this notebook runs on its own.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the fact first: it holds foreign keys that point at the dimensions
# MAGIC DROP TABLE IF EXISTS fact_bookings;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_date
# MAGIC COMMENT 'Calendar dimension, one row per day (Dec 2022 – Dec 2025)'
# MAGIC AS
# MAGIC SELECT d                                AS date_key,
# MAGIC        YEAR(d)                          AS year,
# MAGIC        QUARTER(d)                       AS quarter,
# MAGIC        MONTH(d)                         AS month,
# MAGIC        DATE_FORMAT(d, 'MMM')            AS month_name,
# MAGIC        DATE_FORMAT(d, 'yyyy-MM')        AS year_month,
# MAGIC        DAYOFWEEK(d) IN (1, 7)           AS is_weekend
# MAGIC FROM (SELECT EXPLODE(SEQUENCE(DATE'2022-12-01', DATE'2025-12-31')) AS d);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_property
# MAGIC COMMENT 'Property dimension: listing, destination, country, continent and host attributes'
# MAGIC AS
# MAGIC SELECT p.property_id, p.title, p.property_type, ROUND(p.base_price, 2) AS base_price, p.bedrooms, p.max_guests,
# MAGIC        d.destination, d.country AS destination_country, c.continent,
# MAGIC        h.host_id, h.name AS host_name, ROUND(h.rating, 2) AS host_rating, h.is_verified AS host_is_verified
# MAGIC FROM samples.wanderbricks.properties   p
# MAGIC JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
# MAGIC LEFT JOIN samples.wanderbricks.countries c ON d.country = c.country
# MAGIC JOIN samples.wanderbricks.hosts        h ON p.host_id = h.host_id;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_user
# MAGIC COMMENT 'Guest dimension (no PII: email and name intentionally excluded)'
# MAGIC AS
# MAGIC SELECT u.user_id, u.user_type, u.is_business, u.country AS user_country, c.continent AS user_continent
# MAGIC FROM samples.wanderbricks.users u
# MAGIC LEFT JOIN samples.wanderbricks.countries c ON u.country = c.country;
# MAGIC
# MAGIC CREATE TABLE fact_bookings
# MAGIC COMMENT 'Booking fact: one row per booking with value, nights and status flags'
# MAGIC AS
# MAGIC SELECT booking_id, user_id, property_id,
# MAGIC        TO_DATE(created_at)                         AS booking_date,
# MAGIC        check_in, check_out,
# MAGIC        DATEDIFF(check_out, check_in)               AS nights,
# MAGIC        guests_count,
# MAGIC        CAST(total_amount AS DECIMAL(12,2))         AS total_amount,
# MAGIC        status,
# MAGIC        status IN ('confirmed', 'completed')        AS is_realised,
# MAGIC        status = 'cancelled'                        AS is_cancelled
# MAGIC FROM samples.wanderbricks.bookings;
# MAGIC
# MAGIC SELECT 'dim_date' AS t, COUNT(*) AS rows FROM dim_date
# MAGIC UNION ALL SELECT 'dim_property', COUNT(*) FROM dim_property
# MAGIC UNION ALL SELECT 'dim_user', COUNT(*) FROM dim_user
# MAGIC UNION ALL SELECT 'fact_bookings', COUNT(*) FROM fact_bookings;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keys & comments: teach the tools your model
# MAGIC **Primary/foreign keys** (informational) let Power BI auto-detect relationships and help Genie join correctly. **Comments** are the #1 input Genie uses to understand business meaning.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dim_date      ALTER COLUMN date_key    SET NOT NULL;
# MAGIC ALTER TABLE dim_property  ALTER COLUMN property_id SET NOT NULL;
# MAGIC ALTER TABLE dim_user      ALTER COLUMN user_id     SET NOT NULL;
# MAGIC ALTER TABLE fact_bookings ALTER COLUMN booking_id  SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE dim_date      ADD CONSTRAINT pk_dim_date      PRIMARY KEY (date_key);
# MAGIC ALTER TABLE dim_property  ADD CONSTRAINT pk_dim_property  PRIMARY KEY (property_id);
# MAGIC ALTER TABLE dim_user      ADD CONSTRAINT pk_dim_user      PRIMARY KEY (user_id);
# MAGIC ALTER TABLE fact_bookings ADD CONSTRAINT pk_fact_bookings PRIMARY KEY (booking_id);
# MAGIC ALTER TABLE fact_bookings ADD CONSTRAINT fk_fb_property FOREIGN KEY (property_id)  REFERENCES dim_property;
# MAGIC ALTER TABLE fact_bookings ADD CONSTRAINT fk_fb_user     FOREIGN KEY (user_id)      REFERENCES dim_user;
# MAGIC ALTER TABLE fact_bookings ADD CONSTRAINT fk_fb_date     FOREIGN KEY (booking_date) REFERENCES dim_date;
# MAGIC
# MAGIC ALTER TABLE fact_bookings ALTER COLUMN total_amount COMMENT 'Booking value in USD (gross, before refunds)';
# MAGIC ALTER TABLE fact_bookings ALTER COLUMN is_realised  COMMENT 'TRUE when status is confirmed or completed: use for revenue';
# MAGIC ALTER TABLE fact_bookings ALTER COLUMN is_cancelled COMMENT 'TRUE when the booking was cancelled';
# MAGIC ALTER TABLE fact_bookings ALTER COLUMN booking_date COMMENT 'Date the booking was made (use check_in for stay date)';
# MAGIC ALTER TABLE fact_bookings ALTER COLUMN nights       COMMENT 'Length of stay in nights';
# MAGIC ALTER TABLE dim_property  ALTER COLUMN continent    COMMENT 'Continent of the destination';
# MAGIC ALTER TABLE dim_property  ALTER COLUMN host_rating  COMMENT 'Host average rating, 1–5';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Analytical SQL for the dashboard
# MAGIC
# MAGIC ### 3a. Revenue trend with month-over-month growth (window function `LAG`)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly AS (
# MAGIC   SELECT dd.year_month, p.continent, SUM(f.total_amount) AS revenue
# MAGIC   FROM fact_bookings f
# MAGIC   JOIN dim_property p ON f.property_id = p.property_id
# MAGIC   JOIN dim_date dd    ON f.booking_date = dd.date_key
# MAGIC   WHERE f.is_realised
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT year_month, continent, revenue,
# MAGIC        ROUND(100 * (revenue / LAG(revenue) OVER (PARTITION BY continent ORDER BY year_month) - 1), 1) AS mom_growth_pct
# MAGIC FROM monthly
# MAGIC WHERE year_month >= '2025-01'
# MAGIC ORDER BY continent, year_month

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 **Visualise it:** click **+** above the result → *Visualization* → Line chart, X = `year_month`, Y = `revenue`, Group by = `continent`.
# MAGIC
# MAGIC ### 3b. Top 3 properties per destination (`QUALIFY`)
# MAGIC `QUALIFY` filters on a window function, so you don't need a sub-query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.destination, p.title, p.host_name, SUM(f.total_amount) AS revenue,
# MAGIC        RANK() OVER (PARTITION BY p.destination ORDER BY SUM(f.total_amount) DESC) AS rnk
# MAGIC FROM fact_bookings f
# MAGIC JOIN dim_property p USING (property_id)
# MAGIC WHERE f.is_realised
# MAGIC GROUP BY ALL
# MAGIC QUALIFY rnk <= 3
# MAGIC ORDER BY destination, rnk
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Subtotals with `ROLLUP`
# MAGIC One query returns destination rows, continent subtotals (destination = NULL) **and** a grand total.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COALESCE(p.continent, '🌍 ALL')            AS continent,
# MAGIC        COALESCE(p.destination, 'Σ subtotal')       AS destination,
# MAGIC        COUNT(*)                                    AS bookings,
# MAGIC        SUM(f.total_amount)                         AS revenue
# MAGIC FROM fact_bookings f
# MAGIC JOIN dim_property p USING (property_id)
# MAGIC WHERE f.is_realised
# MAGIC GROUP BY ROLLUP (p.continent, p.destination)
# MAGIC ORDER BY p.continent NULLS LAST, p.destination NULLS LAST

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3d. Cancellation rate matrix (`PIVOT`)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC   SELECT p.property_type, u.user_type, CAST(f.is_cancelled AS INT) AS cancelled
# MAGIC   FROM fact_bookings f
# MAGIC   JOIN dim_property p USING (property_id)
# MAGIC   JOIN dim_user u     USING (user_id)
# MAGIC )
# MAGIC PIVOT (ROUND(100 * AVG(cancelled), 1) FOR user_type IN ('individual' AS individual_pct, 'business' AS business_pct))
# MAGIC ORDER BY property_type

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Define KPIs once: a metric view
# MAGIC Different dashboards often compute "revenue" differently. A **metric view** defines **dimensions** and **measures** once, in Unity Catalog. Power BI, dashboards, Genie and SQL users all get the *same* numbers.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW mv_booking_metrics
WITH METRICS
LANGUAGE YAML
COMMENT 'Governed booking KPIs: bookings, realised revenue, cancellation rate, ADR'
AS $$
version: 0.1
source: >
  SELECT f.*, p.destination, p.continent, p.property_type, u.user_type
  FROM {FQ}.fact_bookings f
  JOIN {FQ}.dim_property p ON f.property_id = p.property_id
  JOIN {FQ}.dim_user u ON f.user_id = u.user_id
dimensions:
  - name: Booking Month
    expr: DATE_TRUNC('MONTH', booking_date)
  - name: Continent
    expr: continent
  - name: Destination
    expr: destination
  - name: Property Type
    expr: property_type
  - name: Guest Type
    expr: user_type
measures:
  - name: Bookings
    expr: COUNT(1)
  - name: Realised Revenue
    expr: SUM(CASE WHEN is_realised THEN total_amount END)
  - name: Cancellation Rate
    expr: AVG(CASE WHEN is_cancelled THEN 1.0 ELSE 0.0 END)
  - name: Average Daily Rate
    expr: SUM(CASE WHEN is_realised THEN total_amount END) / NULLIF(SUM(CASE WHEN is_realised THEN nights END), 0)
$$""")
print("Metric view mv_booking_metrics created")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query a metric view with MEASURE(); the aggregation logic lives in the view
# MAGIC SELECT `Continent`,
# MAGIC        MEASURE(`Bookings`)                       AS bookings,
# MAGIC        ROUND(MEASURE(`Realised Revenue`), 0)      AS realised_revenue,
# MAGIC        ROUND(100 * MEASURE(`Cancellation Rate`), 1) AS cancel_pct,
# MAGIC        ROUND(MEASURE(`Average Daily Rate`), 2)    AS adr
# MAGIC FROM mv_booking_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY realised_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Query history: who ran what, and how fast?
# MAGIC 🖱️ Sidebar → **Query History**: filter by user, warehouse, status and duration. Click a query → **Query profile** to see where time went (scan, join, shuffle), rows read, and whether the result came from **cache**.
# MAGIC
# MAGIC The same data is in a system table, if your admin granted access:

# COMMAND ----------

try:
    display(spark.sql("""
      SELECT start_time, statement_type, execution_status,
             total_duration_ms, read_rows, LEFT(statement_text, 80) AS statement
      FROM system.query.history
      WHERE executed_by = current_user()
      ORDER BY start_time DESC
      LIMIT 15"""))
except Exception as e:
    print(f"system.query.history not available to you ({type(e).__name__}); use the Query History page in the UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Genie spaces: talk to your data (Module 3.2)
# MAGIC
# MAGIC A **Genie space** lets business users ask questions in plain English. Genie writes the SQL using your **tables, comments, keys, instructions and example queries**, and runs it on a SQL warehouse. Users only ever see data they have permissions for.
# MAGIC
# MAGIC ### 5a. Create the space (UI, about 5 minutes)
# MAGIC 1. Sidebar → **Genie** → **+ New**.
# MAGIC 2. **Data:** add `demo-catalog.<your_schema>`: `fact_bookings`, `dim_property`, `dim_user`, `dim_date` (and the metric view `mv_booking_metrics`).
# MAGIC 3. **Warehouse:** pick the class serverless warehouse.
# MAGIC 4. **Title:** `Wanderbricks Bookings – <your name>`. **Description:** *"Ask about bookings, revenue, cancellations by destination, property type, guest type and time."*
# MAGIC 5. **Instructions** (*Configure → Instructions*), paste:
# MAGIC ```text
# MAGIC - "Revenue" means SUM(total_amount) WHERE is_realised = TRUE, unless the user says "gross".
# MAGIC - "Cancellation rate" = cancelled bookings / all bookings.
# MAGIC - Use booking_date for "when booked", check_in for "when stayed".
# MAGIC - "Last quarter" is relative to 2025-07-31 (the latest data date).
# MAGIC - Round money to 0 decimals and percentages to 1 decimal.
# MAGIC - Destinations are cities/regions (dim_property.destination); countries are dim_property.destination_country.
# MAGIC ```
# MAGIC 6. **SQL queries / Trusted assets:** add the example below as a *trusted* query for "top destinations by revenue".

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ⭐ Example "trusted" SQL to give Genie for: "What are the top 5 destinations by revenue in 2025?"
# MAGIC SELECT p.destination, ROUND(SUM(f.total_amount), 0) AS revenue
# MAGIC FROM fact_bookings f
# MAGIC JOIN dim_property p ON f.property_id = p.property_id
# MAGIC WHERE f.is_realised AND YEAR(f.booking_date) = 2025
# MAGIC GROUP BY p.destination
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Questions to try (and what to watch for)
# MAGIC | Question | What good looks like |
# MAGIC |---|---|
# MAGIC | *What are the top 5 destinations by revenue in 2025?* | Uses `is_realised`, matches the trusted query |
# MAGIC | *Which property types have the highest cancellation rate?* | Ski Resort / Historical Place vs Urban; uses `is_cancelled` |
# MAGIC | *Show monthly bookings for business vs individual guests in 2025 as a line chart* | Joins `dim_user`, produces a chart |
# MAGIC | *Who are the top 10 hosts in Asia by revenue?* | Joins `dim_property`, filters `continent = 'Asia'` |
# MAGIC | *What was revenue last quarter?* | Applies the "relative to 2025-07-31" instruction |
# MAGIC | *What's the email of the top guest?* | Can't answer: PII isn't in the model ✅ |
# MAGIC
# MAGIC ### 5c. Prompt curation loop
# MAGIC 1. Ask → **Show code** to read the generated SQL. Is it right?
# MAGIC 2. Wrong? 👎 → add an **instruction**, a **column comment**, or a **trusted SQL** example. Don't just rephrase the question.
# MAGIC 3. Use **Benchmarks** (*Configure → Benchmarks*) to save questions with expected SQL, and re-run them after every change.
# MAGIC 4. **Share** the space with the regional managers' group (they need `SELECT` on the tables + `CAN USE` on the warehouse).
# MAGIC
# MAGIC ---
# MAGIC ## 6. Power BI connectivity (Module 3.3)
# MAGIC
# MAGIC ### 6a. Storage modes
# MAGIC | | **Import** | **DirectQuery** | **Dual / Composite** |
# MAGIC |---|---|---|---|
# MAGIC | Where data lives | Copied into the Power BI dataset (VertiPaq) | Stays in Databricks; every visual sends SQL | Dimensions imported, facts DirectQuery |
# MAGIC | Freshness | As of the last scheduled refresh | **Live** | Live facts |
# MAGIC | Speed | Very fast visuals | Depends on the warehouse (serverless + Photon + caching help) | Fast filters, live numbers |
# MAGIC | Size limit | Capacity memory (1–400 GB) | None | |
# MAGIC | Governance | Power BI security after import | **Unity Catalog permissions per user** (with SSO) | Mixed |
# MAGIC | Use when… | Small/medium data, heavy interactivity | Large or fast-changing data, row-level security in UC | **Most enterprise star schemas** |
# MAGIC
# MAGIC **Wanderbricks recommendation:** `dim_*` tables → **Import** (or Dual); `fact_bookings` → **DirectQuery**, i.e. a *composite* model.
# MAGIC
# MAGIC ### 6b. Connect, option 1: Publish from Databricks (fastest, "zero-driver")
# MAGIC 1. **Catalog Explorer** → `fact_bookings` (or the schema) → **Use with BI tools** → **Publish to Power BI workspace**.
# MAGIC 2. Sign in with Entra ID, pick the Power BI workspace, and choose **DirectQuery** or **Import**.
# MAGIC 3. Databricks creates the semantic model **with the PK/FK relationships** you defined in §2.
# MAGIC
# MAGIC ### 6c. Connect, option 2: Power BI Desktop
# MAGIC 1. In Databricks: **SQL Warehouses → your warehouse → Connection details**. Copy **Server hostname** and **HTTP path**.
# MAGIC 2. Power BI Desktop → **Get data → Azure Databricks**.
# MAGIC 3. Paste the hostname + HTTP path → choose **Import** or **DirectQuery**.
# MAGIC 4. Authenticate with **Microsoft Entra ID** (preferred; personal access tokens only for testing).
# MAGIC 5. Navigator → `demo-catalog` → your schema → select the star-schema tables → **Load**.
# MAGIC 6. Model view: the relationships come from the FK constraints. Set dims to **Dual** and the fact to **DirectQuery** for a composite model.
# MAGIC
# MAGIC > The Azure Databricks connector ships inside Power BI (ADBC-based), so there's no ODBC driver to install. That's the "zero-driver" experience.
# MAGIC
# MAGIC ### 6d. Performance checklist for DirectQuery
# MAGIC - Use a **serverless** warehouse sized for concurrency (scale *out* with max clusters).
# MAGIC - Model as a **star schema** with keys (done ✅); avoid wide flat tables.
# MAGIC - Pre-aggregate heavy visuals into gold tables or use the **metric view**.
# MAGIC - Enable **"Assume referential integrity"** on relationships, which gives inner joins and faster SQL.
# MAGIC - Check **Query History**: every Power BI visual shows up there with its duration.
# MAGIC
# MAGIC ---
# MAGIC ## ✅ Day 3 recap
# MAGIC | You did | Concept |
# MAGIC |---|---|
# MAGIC | Compared warehouse types & settings | SQL warehouses |
# MAGIC | Built `dim_*` + `fact_bookings` with PK/FK and comments | BI-ready gold modelling |
# MAGIC | `LAG`, `QUALIFY`, `ROLLUP`, `PIVOT` | Analytical SQL |
# MAGIC | `mv_booking_metrics` + `MEASURE()` | One governed definition of each KPI |
# MAGIC | Query history | Monitoring BI workloads |
# MAGIC | Genie instructions, trusted SQL, benchmarks | Natural-language analytics |
# MAGIC | Import vs DirectQuery vs composite | Power BI connectivity |
# MAGIC
# MAGIC ### 🧠 Capstone warm-up
# MAGIC 1. 200 analysts hit the dashboard at 9 AM and queries queue up. Which warehouse setting do you change?
# MAGIC 2. Genie keeps calling *gross* bookings "revenue". What are two ways to fix that?
# MAGIC 3. Finance needs numbers that are correct to the minute, and the fact table has 2 billion rows. Import or DirectQuery?
# MAGIC 4. Why did we exclude `email` from `dim_user`?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. Increase **max clusters** (scale out for concurrency). A bigger size only speeds up individual queries.
# MAGIC 2. Add an **instruction** that defines revenue, add a **column comment** on `is_realised`, add a **trusted SQL** example, or point Genie at the **metric view**.
# MAGIC 3. **DirectQuery** (or composite with the fact table in DirectQuery).
# MAGIC 4. Data minimisation: BI and Genie users shouldn't be able to reach PII they don't need.
# MAGIC </details>
# MAGIC
# MAGIC ---
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC Keep these if you're doing **03B** (it adds to the same star schema).

# COMMAND ----------

dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"], "Drop objects created here?")
if dbutils.widgets.get("cleanup") == "yes":
    for stmt in ["DROP VIEW IF EXISTS mv_booking_metrics", "DROP TABLE IF EXISTS fact_bookings",
                 "DROP TABLE IF EXISTS dim_property", "DROP TABLE IF EXISTS dim_user", "DROP TABLE IF EXISTS dim_date"]:
        spark.sql(stmt)
        print("✔", stmt)
else:
    print("Cleanup skipped (set widget 'cleanup' = yes to drop Day 3 objects)")
