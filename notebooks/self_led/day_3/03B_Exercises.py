# Databricks notebook source
# MAGIC %md
# MAGIC # 03B · Day 3: Deep-Dive Exercises (self-led)
# MAGIC ### Databricks Catalyst · Guest Experience analytics: SQL, Genie readiness & BI design
# MAGIC
# MAGIC **Time:** 60–90 min · **Prerequisite:** notebook **03A** (star schema in your user schema)
# MAGIC
# MAGIC ### Scenario: the Guest Experience dashboard
# MAGIC > **Request from the Head of Guest Experience:** *"Leadership reporting covers revenue. My team needs visibility into guest **experience**: sentiment when guests contact support, which agents de-escalate effectively, which properties perform well or poorly in reviews, and where users drop off on the website. We also need a Genie space for self-service questions."*
# MAGIC
# MAGIC This notebook is recommended **capstone preparation**: semi-structured data, data cleaning, gold modelling, Genie preparation and BI design.
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
# MAGIC ## Task 1 · Clean the support-message sentiment
# MAGIC `customer_support_logs.messages` is an **array of structs**. Build table **`silver_support_messages`**, one row per message:
# MAGIC `ticket_id, user_id, support_agent_id, ticket_created_at (TIMESTAMP), msg_seq (1, 2, 3…), sender, sentiment_raw, sentiment_bucket, message`
# MAGIC
# MAGIC `sentiment_bucket` rules:
# MAGIC - **negative**: angry, furious, aggressive, negative, passive aggressive, condescending, dismissive, frustrated, pessimistic, skeptical, concerned
# MAGIC - **positive**: positive, friendly, helpful, caring, optimistic, sincere, agreeable, **aggreeable** (a misspelling present in the source data)
# MAGIC - **neutral**: everything else (neutral, "trying to difuse tension"…)
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC - `posexplode(messages)` gives position + element; `pos + 1` = `msg_seq`.
# MAGIC - `created_at` is a string: `TO_TIMESTAMP(created_at)`.
# MAGIC - Normalise first: `LOWER(TRIM(m.sentiment))`, then `CASE WHEN … IN (…) THEN 'negative' …`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 7,600 rows (1,900 tickets × 4 messages); 3 buckets.

# COMMAND ----------

# TODO Task 1

# COMMAND ----------

check("Task 1 · silver_support_messages", lambda: (
    spark.table("silver_support_messages").count() == 7600
    and spark.table("silver_support_messages").select("sentiment_bucket").distinct().count() == 3,
    str({r[0]: r[1] for r in spark.sql("SELECT sentiment_bucket, COUNT(*) FROM silver_support_messages GROUP BY 1").collect()})))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 2 · Agent scorecard: who calms guests down?
# MAGIC Build **`gold_support_agent_scorecard`**, one row per `support_agent_id`:
# MAGIC - `tickets`: distinct tickets handled
# MAGIC - `pct_opening_negative`: % of tickets whose **first user message** is negative
# MAGIC - `pct_closing_negative`: % of tickets whose **last user message** is negative
# MAGIC - `de_escalation_pts` = `pct_opening_negative - pct_closing_negative` (higher is better)
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC Filter `sender = 'user'`, then use `MIN_BY(sentiment_bucket, msg_seq)` / `MAX_BY(sentiment_bucket, msg_seq)` per ticket. Then aggregate per agent with `AVG(CASE WHEN … = 'negative' THEN 100.0 ELSE 0 END)`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 50 agents; the `tickets` column sums to 1,900.

# COMMAND ----------

# TODO Task 2

# COMMAND ----------

check("Task 2 · gold_support_agent_scorecard", lambda: (
    spark.sql("SELECT COUNT(*) = 50 AND SUM(tickets) = 1900 FROM gold_support_agent_scorecard").first()[0],
    spark.sql("SELECT CONCAT('best agent: ', MAX_BY(support_agent_id, de_escalation_pts)) FROM gold_support_agent_scorecard").first()[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 3 · Property experience scorecard
# MAGIC Build **`gold_property_scorecard`**, one row per property (all 18,163), with:
# MAGIC `property_id, title, destination, property_type, bookings, realised_revenue, cancel_rate_pct, review_count, avg_rating, amenity_count, primary_image_url`
# MAGIC - Reviews: **exclude** `is_deleted = true`
# MAGIC - Use `fact_bookings` / `dim_property` from 03A where possible
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC Aggregate each child (`fact_bookings`, `reviews`, `property_amenities`, `property_images WHERE is_primary`) separately in CTEs, then `LEFT JOIN` all of them to `dim_property`. Avoid a **fan-out**.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 18,163 rows; `SUM(bookings)` = 72,247; no `avg_rating` outside 1–5.

# COMMAND ----------

# TODO Task 3

# COMMAND ----------

check("Task 3 · gold_property_scorecard", lambda: (
    spark.sql("SELECT COUNT(*) = 18163 AND SUM(bookings) = 72247 AND COUNT_IF(avg_rating < 1 OR avg_rating > 5) = 0 FROM gold_property_scorecard").first()[0],
    spark.sql("SELECT CONCAT(COUNT(*), ' properties, ', SUM(bookings), ' bookings') FROM gold_property_scorecard").first()[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 4 · Website funnel by device
# MAGIC Using `clickstream` (`event` ∈ search, filter, view, click; device in `metadata.device`), build **`gold_web_funnel`**, one row per device:
# MAGIC `device, searches, filters, views, clicks, view_to_click_pct`
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC `COUNT_IF(event = 'search')` … or `PIVOT`. `ROUND(100 * clicks / views, 1)`.
# MAGIC Remember: this data covers only about 2 minutes, so do not build a time trend.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** 3 rows (desktop, mobile, tablet); the sum of all event columns = 100,000.

# COMMAND ----------

# TODO Task 4

# COMMAND ----------

check("Task 4 · gold_web_funnel", lambda: (
    spark.sql("SELECT COUNT(*) = 3 AND SUM(searches + filters + views + clicks) = 100000 FROM gold_web_funnel").first()[0],
    "3 devices"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 5 · Make it Genie-ready
# MAGIC Genie is only as good as your metadata. For **`gold_property_scorecard`**, **`gold_support_agent_scorecard`** and **`gold_web_funnel`**:
# MAGIC 1. Add a **table comment** that explains the grain and the business use.
# MAGIC 2. Add **column comments** to every metric column (for example, what `de_escalation_pts` means).
# MAGIC 3. Add a **primary key** on `gold_property_scorecard(property_id)` (remember `NOT NULL` first).
# MAGIC
# MAGIC <details><summary>Hint</summary>
# MAGIC
# MAGIC `COMMENT ON TABLE t IS '…'` · `ALTER TABLE t ALTER COLUMN c COMMENT '…'` · `ALTER TABLE t ALTER COLUMN c SET NOT NULL` · `ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (c)`.
# MAGIC </details>
# MAGIC
# MAGIC **Expected:** all three tables have a comment; `gold_property_scorecard` has a PRIMARY KEY.

# COMMAND ----------

# TODO Task 5

# COMMAND ----------

check("Task 5 · Genie-ready metadata", lambda: (
    spark.sql(f"""SELECT COUNT(*) FROM `{CATALOG}`.information_schema.tables
                  WHERE table_schema = '{USER_SCHEMA}'
                    AND table_name IN ('gold_property_scorecard','gold_support_agent_scorecard','gold_web_funnel')
                    AND comment IS NOT NULL""").first()[0] == 3
    and spark.sql(f"""SELECT COUNT(*) FROM `{CATALOG}`.information_schema.table_constraints
                      WHERE table_schema = '{USER_SCHEMA}' AND table_name = 'gold_property_scorecard'
                        AND constraint_type = 'PRIMARY KEY'""").first()[0] == 1,
    "comments + PK found"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 6 · Write "trusted SQL" answers
# MAGIC Genie benchmarks need correct reference answers. Create **three views**, each answering one question:
# MAGIC
# MAGIC | View | Question |
# MAGIC |---|---|
# MAGIC | `q1_best_rated_destination` | Which **destination** has the highest average property rating, among properties with ≥ 5 reviews? (1 row: destination, avg_rating) |
# MAGIC | `q2_agent_most_negative` | Which **support agent** has the highest `pct_opening_negative`? (1 row) |
# MAGIC | `q3_best_device` | Which **device** has the best `view_to_click_pct`? (1 row) |
# MAGIC
# MAGIC **Expected:** each view returns exactly 1 row.

# COMMAND ----------

# TODO Task 6

# COMMAND ----------

check("Task 6 · trusted SQL views", lambda: (
    all(spark.table(v).count() == 1 for v in ["q1_best_rated_destination", "q2_agent_most_negative", "q3_best_device"]),
    "; ".join(str(spark.table(v).first().asDict()) for v in ["q1_best_rated_destination", "q2_agent_most_negative", "q3_best_device"])))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Task 7 · Power BI design decisions (discussion, no code)
# MAGIC For each request, pick **Import**, **DirectQuery** or **Composite**, and say why. Write your answers in this markdown cell (double-click to edit):
# MAGIC
# MAGIC | # | Request | Your choice & why |
# MAGIC |---|---|---|
# MAGIC | a | Exec KPI dashboard on `gold_support_agent_scorecard` (50 rows), refreshed daily | |
# MAGIC | b | Operations monitor showing bookings in the last hour from a 2-billion-row fact table | |
# MAGIC | c | Regional managers must only see their own region's rows, enforced by a Unity Catalog row filter | |
# MAGIC | d | Property explorer: `dim_property` slicers (18K rows) + `fact_bookings` details | |
# MAGIC
# MAGIC Then implement it: **Publish to Power BI** from Catalog Explorer, *or* create a **Genie space** on your three gold tables, add your `q*` views as **trusted SQL**, and ask:
# MAGIC - *"Which agents have the best de-escalation?"*
# MAGIC - *"Show average rating by property type for Paris"*
# MAGIC - *"Which device converts views to clicks best?"*
# MAGIC
# MAGIC ---
# MAGIC ## Reflection questions
# MAGIC 1. In Task 3, what would happen to `review_count` if you joined `reviews` and `property_amenities` before aggregating?
# MAGIC 2. Why did we bucket sentiment in **silver** rather than in each dashboard?
# MAGIC 3. `reviews.review_id` is not unique in the source (see notebook 00). Does that affect `review_count`? How would you check?
# MAGIC 4. What is one question Genie *should not* be able to answer from these tables, and why is that good?
# MAGIC
# MAGIC ---
# MAGIC ---
# MAGIC # Solutions
# MAGIC > **Note:** Attempt the tasks before reviewing this section.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_support_messages
# MAGIC COMMENT 'Silver: one row per support message with normalised sentiment bucket'
# MAGIC AS
# MAGIC WITH exploded AS (
# MAGIC   SELECT ticket_id, user_id, support_agent_id,
# MAGIC          TO_TIMESTAMP(created_at) AS ticket_created_at,
# MAGIC          pos + 1                  AS msg_seq,
# MAGIC          m.sender, m.sentiment    AS sentiment_raw, m.message
# MAGIC   FROM samples.wanderbricks.customer_support_logs
# MAGIC   LATERAL VIEW POSEXPLODE(messages) t AS pos, m
# MAGIC )
# MAGIC SELECT ticket_id, user_id, support_agent_id, ticket_created_at, msg_seq, sender, sentiment_raw,
# MAGIC        CASE WHEN LOWER(TRIM(sentiment_raw)) IN ('angry','furious','aggressive','negative','passive aggressive','condescending',
# MAGIC                                                 'dismissive','frustrated','pessimistic','skeptical','concerned') THEN 'negative'
# MAGIC             WHEN LOWER(TRIM(sentiment_raw)) IN ('positive','friendly','helpful','caring','optimistic','sincere',
# MAGIC                                                 'agreeable','aggreeable') THEN 'positive'
# MAGIC             ELSE 'neutral' END AS sentiment_bucket,
# MAGIC        message
# MAGIC FROM exploded;
# MAGIC
# MAGIC SELECT sender, sentiment_bucket, COUNT(*) AS messages FROM silver_support_messages GROUP BY ALL ORDER BY 1, 3 DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_support_agent_scorecard AS
# MAGIC WITH per_ticket AS (
# MAGIC   SELECT ticket_id, support_agent_id,
# MAGIC          MIN_BY(sentiment_bucket, msg_seq) AS opening,
# MAGIC          MAX_BY(sentiment_bucket, msg_seq) AS closing
# MAGIC   FROM silver_support_messages
# MAGIC   WHERE sender = 'user'
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT support_agent_id,
# MAGIC        COUNT(*)                                                            AS tickets,
# MAGIC        ROUND(AVG(CASE WHEN opening = 'negative' THEN 100.0 ELSE 0 END), 1) AS pct_opening_negative,
# MAGIC        ROUND(AVG(CASE WHEN closing = 'negative' THEN 100.0 ELSE 0 END), 1) AS pct_closing_negative,
# MAGIC        ROUND(AVG(CASE WHEN opening = 'negative' THEN 100.0 ELSE 0 END)
# MAGIC            - AVG(CASE WHEN closing = 'negative' THEN 100.0 ELSE 0 END), 1) AS de_escalation_pts
# MAGIC FROM per_ticket
# MAGIC GROUP BY support_agent_id;
# MAGIC
# MAGIC SELECT * FROM gold_support_agent_scorecard ORDER BY de_escalation_pts DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 3

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_property_scorecard AS
# MAGIC WITH bk AS (
# MAGIC   SELECT property_id, COUNT(*) AS bookings,
# MAGIC          SUM(CASE WHEN is_realised THEN total_amount END)              AS realised_revenue,
# MAGIC          ROUND(100 * AVG(CASE WHEN is_cancelled THEN 1 ELSE 0 END), 1) AS cancel_rate_pct
# MAGIC   FROM fact_bookings GROUP BY property_id
# MAGIC ), rv AS (
# MAGIC   SELECT property_id, COUNT(*) AS review_count, ROUND(AVG(rating), 2) AS avg_rating
# MAGIC   FROM samples.wanderbricks.reviews WHERE NOT is_deleted GROUP BY property_id
# MAGIC ), am AS (
# MAGIC   SELECT property_id, COUNT(*) AS amenity_count FROM samples.wanderbricks.property_amenities GROUP BY property_id
# MAGIC ), img AS (
# MAGIC   SELECT property_id, MIN(url) AS primary_image_url FROM samples.wanderbricks.property_images WHERE is_primary GROUP BY property_id
# MAGIC )
# MAGIC SELECT p.property_id, p.title, p.destination, p.property_type,
# MAGIC        COALESCE(bk.bookings, 0)         AS bookings,
# MAGIC        COALESCE(bk.realised_revenue, 0) AS realised_revenue,
# MAGIC        bk.cancel_rate_pct,
# MAGIC        COALESCE(rv.review_count, 0)     AS review_count,
# MAGIC        rv.avg_rating,
# MAGIC        COALESCE(am.amenity_count, 0)    AS amenity_count,
# MAGIC        img.primary_image_url
# MAGIC FROM dim_property p
# MAGIC LEFT JOIN bk  ON p.property_id = bk.property_id
# MAGIC LEFT JOIN rv  ON p.property_id = rv.property_id
# MAGIC LEFT JOIN am  ON p.property_id = am.property_id
# MAGIC LEFT JOIN img ON p.property_id = img.property_id;
# MAGIC
# MAGIC SELECT * FROM gold_property_scorecard ORDER BY realised_revenue DESC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 4

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_web_funnel AS
# MAGIC SELECT metadata.device               AS device,
# MAGIC        COUNT_IF(event = 'search')    AS searches,
# MAGIC        COUNT_IF(event = 'filter')    AS filters,
# MAGIC        COUNT_IF(event = 'view')      AS views,
# MAGIC        COUNT_IF(event = 'click')     AS clicks,
# MAGIC        ROUND(100 * COUNT_IF(event = 'click') / COUNT_IF(event = 'view'), 1) AS view_to_click_pct
# MAGIC FROM samples.wanderbricks.clickstream
# MAGIC GROUP BY metadata.device;
# MAGIC
# MAGIC SELECT * FROM gold_web_funnel ORDER BY device;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 5

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE gold_property_scorecard IS 'Guest Experience: one row per property with bookings, revenue, cancellation rate, review rating and amenities. Use for property performance questions.';
# MAGIC COMMENT ON TABLE gold_support_agent_scorecard IS 'Guest Experience: one row per support agent. Measures how often guests open and close tickets with negative sentiment.';
# MAGIC COMMENT ON TABLE gold_web_funnel IS 'Website funnel per device (search > filter > view > click) from clickstream. Snapshot only, not a time series.';
# MAGIC
# MAGIC ALTER TABLE gold_property_scorecard ALTER COLUMN realised_revenue COMMENT 'USD from confirmed + completed bookings';
# MAGIC ALTER TABLE gold_property_scorecard ALTER COLUMN cancel_rate_pct  COMMENT 'Cancelled bookings as % of all bookings';
# MAGIC ALTER TABLE gold_property_scorecard ALTER COLUMN avg_rating       COMMENT 'Average guest rating 1–5, excluding deleted reviews';
# MAGIC ALTER TABLE gold_property_scorecard ALTER COLUMN review_count     COMMENT 'Number of non-deleted reviews';
# MAGIC ALTER TABLE gold_support_agent_scorecard ALTER COLUMN pct_opening_negative COMMENT '% of tickets where the guest''s first message was negative';
# MAGIC ALTER TABLE gold_support_agent_scorecard ALTER COLUMN pct_closing_negative COMMENT '% of tickets where the guest''s last message was negative';
# MAGIC ALTER TABLE gold_support_agent_scorecard ALTER COLUMN de_escalation_pts    COMMENT 'Opening minus closing negative %. Higher = agent calms guests down better';
# MAGIC ALTER TABLE gold_web_funnel ALTER COLUMN view_to_click_pct COMMENT 'Clicks as % of property views';
# MAGIC
# MAGIC ALTER TABLE gold_property_scorecard ALTER COLUMN property_id SET NOT NULL;
# MAGIC ALTER TABLE gold_property_scorecard ADD CONSTRAINT pk_gold_property_scorecard PRIMARY KEY (property_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 6

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW q1_best_rated_destination AS
# MAGIC SELECT destination, ROUND(AVG(avg_rating), 2) AS avg_rating
# MAGIC FROM gold_property_scorecard
# MAGIC WHERE review_count >= 5
# MAGIC GROUP BY destination
# MAGIC ORDER BY avg_rating DESC
# MAGIC LIMIT 1;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW q2_agent_most_negative AS
# MAGIC SELECT support_agent_id, pct_opening_negative
# MAGIC FROM gold_support_agent_scorecard
# MAGIC ORDER BY pct_opening_negative DESC, support_agent_id
# MAGIC LIMIT 1;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW q3_best_device AS
# MAGIC SELECT device, view_to_click_pct
# MAGIC FROM gold_web_funnel
# MAGIC ORDER BY view_to_click_pct DESC
# MAGIC LIMIT 1;
# MAGIC
# MAGIC SELECT 'q1' AS q, destination AS answer, CAST(avg_rating AS STRING) AS value FROM q1_best_rated_destination
# MAGIC UNION ALL SELECT 'q2', support_agent_id, CAST(pct_opening_negative AS STRING) FROM q2_agent_most_negative
# MAGIC UNION ALL SELECT 'q3', device, CAST(view_to_click_pct AS STRING) FROM q3_best_device;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution 7 (one good answer)
# MAGIC | # | Choice | Why |
# MAGIC |---|---|---|
# MAGIC | a | **Import** | Tiny table, daily refresh is fine, fastest visuals |
# MAGIC | b | **DirectQuery** | Needs near-real-time data, and the table is far too large to import |
# MAGIC | c | **DirectQuery with SSO** | The UC row filter is evaluated *per user* only when queries run live as that user. Imported data would bypass it |
# MAGIC | d | **Composite** | `dim_property` in Import/Dual for fast slicers; `fact_bookings` in DirectQuery |
# MAGIC
# MAGIC ### Reflection answers
# MAGIC 1. **Fan-out:** each review row would be repeated once per amenity, which inflates `review_count`. Aggregate first, then join.
# MAGIC 2. One definition, reused everywhere. Dashboards, Genie and Power BI all agree, and fixing a typo happens once.
# MAGIC 3. `COUNT(*)` counts rows, so duplicated review rows inflate it. Compare `COUNT(*)` vs `COUNT(DISTINCT review_id, booking_id)` per property, and dedupe in silver if needed.
# MAGIC 4. "What is the guest's email or phone?" PII is not in the gold model, so Genie *cannot* leak it (data minimisation).
# MAGIC
# MAGIC ### Re-run all checks

# COMMAND ----------

dbutils.widgets.dropdown("validate", "no", ["no", "yes"], "Fail notebook if a check fails?")
check("Task 1 · silver_support_messages", lambda: (spark.table("silver_support_messages").count() == 7600, None))
check("Task 2 · gold_support_agent_scorecard", lambda: (spark.table("gold_support_agent_scorecard").count() == 50, None))
check("Task 3 · gold_property_scorecard", lambda: (spark.table("gold_property_scorecard").count() == 18163, None))
check("Task 4 · gold_web_funnel", lambda: (spark.table("gold_web_funnel").count() == 3, None))
check("Task 5 · Genie-ready metadata", lambda: (spark.sql(f"SELECT COUNT(*) FROM `{CATALOG}`.information_schema.table_constraints WHERE table_schema = '{USER_SCHEMA}' AND table_name = 'gold_property_scorecard' AND constraint_type = 'PRIMARY KEY'").first()[0] == 1, None))
check("Task 6 · trusted SQL views", lambda: (all(spark.table(v).count() == 1 for v in ["q1_best_rated_destination", "q2_agent_most_negative", "q3_best_device"]), None))
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
    for stmt in ["DROP VIEW IF EXISTS q1_best_rated_destination", "DROP VIEW IF EXISTS q2_agent_most_negative",
                 "DROP VIEW IF EXISTS q3_best_device", "DROP TABLE IF EXISTS gold_web_funnel",
                 "DROP TABLE IF EXISTS gold_property_scorecard", "DROP TABLE IF EXISTS gold_support_agent_scorecard",
                 "DROP TABLE IF EXISTS silver_support_messages"]:
        spark.sql(stmt)
        print("Executed:", stmt)
else:
    print("Cleanup skipped (set widget 'cleanup' = yes to drop Day 3B objects)")
