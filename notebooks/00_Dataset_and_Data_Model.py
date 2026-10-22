# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Wanderbricks: Dataset & Data Model
# MAGIC ### Databricks Catalyst · Pre-read for all three days
# MAGIC
# MAGIC **Audience:** all participants · **Time:** 20–30 min · **Compute:** Serverless
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🎯 Learning objectives
# MAGIC By the end of this notebook you will be able to:
# MAGIC - Describe the **Wanderbricks** business and the questions its data teams need to answer
# MAGIC - Name each of the **16 tables** in `samples.wanderbricks`, what one row represents, and its key columns
# MAGIC - Read the **entity-relationship (ER) diagram** and join tables correctly
# MAGIC - Recognise the **data-quality traps** in this dataset that we'll fix during Days 1–3
# MAGIC
# MAGIC ### 🏢 Business context
# MAGIC **Wanderbricks** is a fictional online **vacation-rental marketplace**, similar to Airbnb or Vrbo.
# MAGIC - **Hosts** list **properties** in 42 world **destinations**.
# MAGIC - **Users** (individual travellers and business accounts) browse the website (**page views**, **clickstream**), make **bookings**, **pay**, and leave **reviews**.
# MAGIC - Bookings change over time (pending → confirmed → completed / cancelled), and every change is captured in **booking_updates**, a change-data-capture (CDC) feed.
# MAGIC - Hosts employ **staff** (cleaners, chefs, security…), and users talk to **customer support** agents.
# MAGIC
# MAGIC Every demo in this training is a request from one of the Wanderbricks teams: Finance, Data Engineering, Trust & Safety, Guest Experience, or Leadership.

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
# MAGIC ## 1. What's in the schema?
# MAGIC
# MAGIC The `information_schema` is Unity Catalog's built-in, queryable metadata catalog. Below we list every table in `samples.wanderbricks` along with the **description its owner wrote** (the table comment). Good comments like these are what make data **discoverable** (Day 1) and **Genie-ready** (Day 3).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, table_type, data_source_format, comment
# MAGIC FROM samples.information_schema.tables
# MAGIC WHERE table_schema = 'wanderbricks'
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row counts
# MAGIC Volumes range from 38 amenities to 500K page views. The cell below builds one `UNION ALL` query from Python, which is a common pattern when you need the same statement for many tables.

# COMMAND ----------

tables = [r.table_name for r in spark.sql(
    "SELECT table_name FROM samples.information_schema.tables WHERE table_schema='wanderbricks' ORDER BY 1").collect()]

counts_sql = " UNION ALL ".join(
    f"SELECT '{t}' AS table_name, COUNT(*) AS row_count FROM {SRC}.{t}" for t in tables)
display(spark.sql(counts_sql + " ORDER BY row_count DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table catalogue
# MAGIC
# MAGIC | Group | Table | One row = | Key columns | Notes |
# MAGIC |---|---|---|---|---|
# MAGIC | **Reference** | `countries` | a country | `country` (PK, name), `country_code`, `continent` | 168 rows. Joined **by name**, not by code. |
# MAGIC | | `destinations` | a destination city/region | `destination_id` (PK), `country` | 42 rows. `description` is long Markdown text. |
# MAGIC | | `amenities` | an amenity (Wi-Fi, Pool…) | `amenity_id` (PK), `category` | 38 rows |
# MAGIC | **Supply** | `hosts` | a host (property owner) | `host_id` (PK), `country`, `rating`, `is_verified`, `is_active` | Contains PII: `email`, `phone` |
# MAGIC | | `properties` | a listing | `property_id` (PK), `host_id` (FK), `destination_id` (FK), `base_price`, `property_type` | 4 property types |
# MAGIC | | `property_amenities` | property ↔ amenity link | (`property_id`, `amenity_id`) | Bridge table for the many-to-many relationship |
# MAGIC | | `property_images` | an image of a property | `image_id`, `property_id`, `is_primary` | |
# MAGIC | | `employees` | a host's staff member | `employee_id`, `host_id` (FK), `role`, `is_currently_employed` | Every host also appears as an `owner` row |
# MAGIC | **Demand** | `users` | a customer account | `user_id` (PK), `user_type` (individual / business), `country` | Contains PII: `email` |
# MAGIC | | `bookings` | a reservation (current state) | `booking_id` (PK), `user_id`, `property_id`, `check_in`, `check_out`, `total_amount`, `status` | Dec 2022 – Jul 2025 |
# MAGIC | | `booking_updates` | one **change** to a booking | `booking_update_id`, `booking_id`, `status`, `updated_at` | CDC feed: about 1.7 updates per booking |
# MAGIC | | `payments` | a payment attempt | `payment_id`, `booking_id`, `amount`, `payment_method`, `status` | completed / failed / refunded |
# MAGIC | | `reviews` | a guest review | `review_id`, `booking_id`, `property_id`, `user_id`, `rating`, `is_deleted` | Soft deletes |
# MAGIC | **Engagement** | `page_views` | a page view on a property | `view_id`, `user_id`, `property_id`, `device_type`, `referrer` | 500K rows covering about **2 minutes** |
# MAGIC | | `clickstream` | a website event | `event` (search / filter / view / click), `metadata` **struct**, `user_id`, `property_id` | Semi-structured |
# MAGIC | | `customer_support_logs` | a support ticket | `ticket_id` (PK), `user_id`, `support_agent_id`, `messages` **array&lt;struct&gt;** | Each message has a sentiment label |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Entity-relationship (ER) diagram
# MAGIC
# MAGIC `||--o{` means **one-to-many**: for example, one host has zero or more properties.
# MAGIC
# MAGIC ```mermaid
# MAGIC erDiagram
# MAGIC   COUNTRIES ||--o{ DESTINATIONS : "country (name)"
# MAGIC   COUNTRIES ||--o{ USERS : "country (name)"
# MAGIC   COUNTRIES ||--o{ HOSTS : "country (name)"
# MAGIC   DESTINATIONS ||--o{ PROPERTIES : destination_id
# MAGIC   HOSTS ||--o{ PROPERTIES : host_id
# MAGIC   HOSTS ||--o{ EMPLOYEES : host_id
# MAGIC   PROPERTIES ||--o{ PROPERTY_AMENITIES : property_id
# MAGIC   AMENITIES ||--o{ PROPERTY_AMENITIES : amenity_id
# MAGIC   PROPERTIES ||--o{ PROPERTY_IMAGES : property_id
# MAGIC   USERS ||--o{ BOOKINGS : user_id
# MAGIC   PROPERTIES ||--o{ BOOKINGS : property_id
# MAGIC   BOOKINGS ||--o{ BOOKING_UPDATES : booking_id
# MAGIC   BOOKINGS ||--o{ PAYMENTS : booking_id
# MAGIC   BOOKINGS ||--o{ REVIEWS : booking_id
# MAGIC   USERS ||--o{ CUSTOMER_SUPPORT_LOGS : user_id
# MAGIC   USERS ||--o{ CLICKSTREAM : user_id
# MAGIC   PROPERTIES ||--o{ CLICKSTREAM : property_id
# MAGIC   USERS ||--o{ PAGE_VIEWS : user_id
# MAGIC   PROPERTIES ||--o{ PAGE_VIEWS : property_id
# MAGIC ```
# MAGIC
# MAGIC If the diagram above shows as plain text in your workspace, run the next cell, which renders the same diagram with attributes included.

# COMMAND ----------

er = """
erDiagram
  COUNTRIES ||--o{ DESTINATIONS : "country name"
  DESTINATIONS ||--o{ PROPERTIES : destination_id
  HOSTS ||--o{ PROPERTIES : host_id
  HOSTS ||--o{ EMPLOYEES : host_id
  PROPERTIES ||--o{ PROPERTY_AMENITIES : property_id
  AMENITIES ||--o{ PROPERTY_AMENITIES : amenity_id
  PROPERTIES ||--o{ PROPERTY_IMAGES : property_id
  USERS ||--o{ BOOKINGS : user_id
  PROPERTIES ||--o{ BOOKINGS : property_id
  BOOKINGS ||--o{ BOOKING_UPDATES : booking_id
  BOOKINGS ||--o{ PAYMENTS : booking_id
  BOOKINGS ||--o{ REVIEWS : booking_id
  USERS ||--o{ CUSTOMER_SUPPORT_LOGS : user_id
  USERS ||--o{ CLICKSTREAM : user_id
  USERS ||--o{ PAGE_VIEWS : user_id
  PROPERTIES ||--o{ PAGE_VIEWS : property_id
  COUNTRIES { string country PK
              string country_code
              string continent }
  DESTINATIONS { bigint destination_id PK
                 string destination
                 string country FK }
  HOSTS { bigint host_id PK
          string name
          string country
          float rating
          boolean is_verified }
  PROPERTIES { bigint property_id PK
               bigint host_id FK
               bigint destination_id FK
               float base_price
               string property_type }
  USERS { bigint user_id PK
          string user_type
          string country
          boolean is_business }
  BOOKINGS { bigint booking_id PK
             bigint user_id FK
             bigint property_id FK
             date check_in
             date check_out
             float total_amount
             string status }
  BOOKING_UPDATES { bigint booking_update_id PK
                    bigint booking_id FK
                    string status
                    timestamp updated_at }
  PAYMENTS { bigint payment_id PK
             bigint booking_id FK
             decimal amount
             string payment_method
             string status }
  REVIEWS { bigint review_id PK
            bigint booking_id FK
            double rating
            boolean is_deleted }
"""
displayHTML(f"""
<div class="mermaid">{er}</div>
<script type="module">
  import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.esm.min.mjs';
  mermaid.initialize({{ startOnLoad: true, theme: 'default' }});
</script>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declared vs enforced keys
# MAGIC The schema owner **declared** primary keys. In Unity Catalog, primary- and foreign-key constraints are **informational**: they document the model and help optimisers and BI tools, but they are **not enforced** on write. The next cell lists the declared constraints.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, constraint_name, constraint_type
# MAGIC FROM samples.information_schema.table_constraints
# MAGIC WHERE table_schema = 'wanderbricks'
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prove the relationships with data
# MAGIC A data engineer never trusts a diagram blindly. A **LEFT ANTI JOIN** returns rows on the left side that have **no match** on the right side, so it finds *orphans*. Zero orphans means the relationship holds.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bookings → users'           AS relationship, COUNT(*) AS orphans FROM samples.wanderbricks.bookings b         LEFT ANTI JOIN samples.wanderbricks.users u        ON b.user_id = u.user_id
# MAGIC UNION ALL SELECT 'bookings → properties',  COUNT(*) FROM samples.wanderbricks.bookings b         LEFT ANTI JOIN samples.wanderbricks.properties p   ON b.property_id = p.property_id
# MAGIC UNION ALL SELECT 'properties → hosts',     COUNT(*) FROM samples.wanderbricks.properties p       LEFT ANTI JOIN samples.wanderbricks.hosts h        ON p.host_id = h.host_id
# MAGIC UNION ALL SELECT 'properties → destinations', COUNT(*) FROM samples.wanderbricks.properties p    LEFT ANTI JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id
# MAGIC UNION ALL SELECT 'destinations → countries', COUNT(*) FROM samples.wanderbricks.destinations d   LEFT ANTI JOIN samples.wanderbricks.countries c    ON d.country = c.country
# MAGIC UNION ALL SELECT 'payments → bookings',    COUNT(*) FROM samples.wanderbricks.payments p         LEFT ANTI JOIN samples.wanderbricks.bookings b     ON p.booking_id = b.booking_id
# MAGIC UNION ALL SELECT 'booking_updates → bookings', COUNT(*) FROM samples.wanderbricks.booking_updates x LEFT ANTI JOIN samples.wanderbricks.bookings b  ON x.booking_id = b.booking_id
# MAGIC UNION ALL SELECT 'reviews → bookings',     COUNT(*) FROM samples.wanderbricks.reviews r          LEFT ANTI JOIN samples.wanderbricks.bookings b     ON r.booking_id = b.booking_id

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the result:** most relationships are clean. However, **8 payments** and **47 booking updates** point to bookings that don't exist. These orphans are real-world issues, and the Day 2 pipelines handle them.
# MAGIC
# MAGIC ## 5. Data-quality profile
# MAGIC Next we save a reusable **data-quality profile** to *your* sandbox as table `dq_profile`. Each row is one check, so we can re-run it later and compare results.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dq_profile
# MAGIC COMMENT 'Data-quality profile of samples.wanderbricks (built in notebook 00)'
# MAGIC AS
# MAGIC SELECT 'reviews'  AS table_name, 'duplicate review_id (declared PK)'  AS check_name, COUNT(*) - COUNT(DISTINCT review_id)  AS issue_count FROM samples.wanderbricks.reviews
# MAGIC UNION ALL SELECT 'page_views', 'duplicate view_id (declared PK)',   COUNT(*) - COUNT(DISTINCT view_id)    FROM samples.wanderbricks.page_views
# MAGIC UNION ALL SELECT 'payments',   'duplicate payment_id (declared PK)',COUNT(*) - COUNT(DISTINCT payment_id) FROM samples.wanderbricks.payments
# MAGIC UNION ALL SELECT 'payments',   'bookings with >1 payment row',      COUNT(*) - COUNT(DISTINCT booking_id) FROM samples.wanderbricks.payments
# MAGIC UNION ALL SELECT 'users',      'duplicate email',                   COUNT(email) - COUNT(DISTINCT email)  FROM samples.wanderbricks.users
# MAGIC UNION ALL SELECT 'users',      'null email',                        COUNT_IF(email IS NULL)               FROM samples.wanderbricks.users
# MAGIC UNION ALL SELECT 'bookings',   'total_amount <= 0',                 COUNT_IF(total_amount <= 0)           FROM samples.wanderbricks.bookings
# MAGIC UNION ALL SELECT 'reviews',    'soft-deleted reviews',              COUNT_IF(is_deleted)                  FROM samples.wanderbricks.reviews
# MAGIC UNION ALL SELECT 'payments',   'amount differs from booking total', COUNT_IF(ABS(p.amount - b.total_amount) > 0.01)
# MAGIC           FROM samples.wanderbricks.payments p JOIN samples.wanderbricks.bookings b USING (booking_id);
# MAGIC
# MAGIC SELECT * FROM dq_profile ORDER BY issue_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Key observations (keep these in mind all week)
# MAGIC | # | Observation | Where we deal with it |
# MAGIC |---|---|---|
# MAGIC | 1 | Declared PKs are **not unique** in `reviews`, `page_views` and `payments`, because constraints are informational only | Day 2 (DQ checks fail the job; we dedupe and repair) |
# MAGIC | 2 | Orphan payments and booking updates | Day 2 (MERGE inserts unknown bookings; reconciliation flags orphans) |
# MAGIC | 3 | `payments.amount` often differs from `bookings.total_amount`, and some bookings have several payment rows (retries, refunds) | Day 2B (payment reconciliation) |
# MAGIC | 4 | PII in `users.email`, `hosts.email`, `hosts.phone`, `employees.email` | Day 1 (masking, dynamic views, row filters) |
# MAGIC | 5 | Geographic **skew**: India and China account for about 37% of users and hosts | Day 3 (careful with averages and "top N" charts) |
# MAGIC | 6 | `page_views` and `clickstream` cover only **about 2 minutes** of activity | Use for device/referrer/funnel analysis only, not time trends |
# MAGIC | 7 | `customer_support_logs.created_at` is a **string**, and sentiment labels are messy (for example, "aggreeable" and "agreeable") | Day 3B (casting and normalising) |
# MAGIC | 8 | Booking dates run from Dec 2022 to Jul 2025 | Use `2025-07-31` as "today" in demos |
# MAGIC
# MAGIC ## 6. Semi-structured columns at a glance
# MAGIC Databricks handles nested data natively: you access a **struct** field with `.` and turn an **array** into rows with `explode`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- STRUCT: clickstream.metadata has device + referrer
# MAGIC SELECT event, metadata.device AS device, metadata.referrer AS referrer, COUNT(*) AS events
# MAGIC FROM samples.wanderbricks.clickstream
# MAGIC GROUP BY ALL
# MAGIC ORDER BY event, device
# MAGIC LIMIT 12

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ARRAY<STRUCT>: one support ticket has several messages
# MAGIC SELECT ticket_id, m.sender, m.sentiment, LEFT(m.message, 90) AS message_preview
# MAGIC FROM samples.wanderbricks.customer_support_logs
# MAGIC LATERAL VIEW explode(messages) t AS m
# MAGIC WHERE ticket_id = (SELECT MIN(ticket_id) FROM samples.wanderbricks.customer_support_logs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The questions each team will ask (our roadmap)
# MAGIC | Day | Team | Question |
# MAGIC |---|---|---|
# MAGIC | 1 | Finance / Security | "Where is the booking data, who can see it, and how do we protect guest emails?" |
# MAGIC | 2 | Data Engineering | "Turn the daily booking-change files into a reliable, scheduled bronze → silver → gold pipeline, and fix it when it breaks." |
# MAGIC | 3 | Leadership / Guest Experience | "Show revenue and host performance in Power BI, and let managers ask questions in plain English with Genie." |
# MAGIC
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC This notebook creates only one table, `dq_profile`, which later notebooks don't need. Set the widget to `yes` and run the cell to drop it.

# COMMAND ----------

dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"], "Drop objects created here?")
if dbutils.widgets.get("cleanup") == "yes":
    spark.sql("DROP TABLE IF EXISTS dq_profile")
    print("Dropped dq_profile")
else:
    print("Cleanup skipped (set widget 'cleanup' = yes to drop dq_profile)")
