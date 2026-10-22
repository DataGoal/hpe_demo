# Databricks Catalyst: Demo Notebooks (Wanderbricks)

Demo and exercise notebooks for the 3-day **Databricks Catalyst** training (Oct 27–29, 2026). They are built on the Databricks sample dataset `samples.wanderbricks`, a fictional vacation-rental marketplace.

## Notebooks

| # | File | Audience | Covers |
|---|------|----------|--------|
| 0 | `notebooks/00_Dataset_and_Data_Model.py` | All (pre-read) | Business domain, all 16 tables, ER diagram, key validation, data-quality profile |
| 1A | `notebooks/01A_Day1_Platform_UC_Governance.py` | Instructor-led | Lakehouse & Delta, compute choices, UC 3-level namespace, discovery, tables/views/volumes/functions, GRANT/REVOKE, dynamic view, secret scopes |
| 1B | `notebooks/01B_Day1_Exercises.py` | Self-led | information_schema discovery, managed table build, Delta time travel + RESTORE, column masks, row filters, grant audit, DEEP CLONE + UNDROP |
| 2A | `notebooks/02A_Day2_Pipeline_and_Workflows.py` | Instructor-led | Widgets, multi-language cells, Assistant (/explain /doc /fix), CTAS + read_files, COPY INTO, Auto Loader, medallion + MERGE, SDK-defined multi-task job, run monitoring, failure + repair run |
| 2B | `notebooks/02B_Day2_Exercises.py` | Self-led | Parameterised ingestion, DQ profiling, DQ-gate job that fails on real duplicates, fix + repair, payment reconciliation, run report / system tables |
| 3A | `notebooks/03A_Day3_SQL_Genie_PowerBI.py` | Instructor-led | SQL warehouses, star schema with PK/FK + comments, window functions / QUALIFY / ROLLUP / PIVOT, metric view, query history, Genie space setup guide, Power BI Import vs DirectQuery guide |
| 3B | `notebooks/03B_Day3_Exercises.py` | Self-led | Array-of-struct sentiment cleaning, agent and property scorecards, web funnel, Genie-ready metadata, trusted SQL, Power BI design decisions |
| | `notebooks/day2_tasks/t1_ingest.py` … `t4_dq_check.py` | Used by 2A/2B | Task notebooks that the Day 2 jobs run (ingest → merge → gold ‖ DQ check). **Import them with the folder; do not run them by hand.** |

## Run order
`00` → `01A` → `01B` → `02A` → `02B` → `03A` → `03B`

- 01B, 02B and 03B are self-contained, but follow-on tasks assume the matching A notebook ran first (02B also needs `day2_tasks/`).
- B notebooks: participants fill in the `# TODO` cells; **solutions are in a section at the end**. Each task has a check cell that prints PASS or FAIL.

## Importing
1. In the workspace, go to **Workspace → your folder → ⋮ → Import**, and import the whole `notebooks/` folder (keep the `day2_tasks/` sub-folder next to the 02 notebooks).
   - Or use the CLI: `databricks workspace import-dir notebooks /Users/<you>/catalyst`
2. Attach to **Serverless** compute and **Run all**.

## Prerequisites
| Requirement | Why |
|---|---|
| Serverless compute for notebooks **and** jobs | All notebooks and the Day 2 jobs run on serverless |
| `USE CATALOG` + `CREATE SCHEMA` on `` `demo-catalog` `` | Each participant creates their own schema |
| `USE CATALOG`/`USE SCHEMA`/`SELECT` on `samples.wanderbricks` | Source data (read-only) |
| Permission to create jobs | 02A and 02B create one job each |
| *(optional)* `SELECT` on `system.query.history`, `system.lakeflow` | Optional monitoring cells; they fall back gracefully |
| *(optional)* A SQL warehouse + Genie access + Power BI Desktop | Day 3 UI walkthroughs |

Secret scopes are covered **as a concept** (the code is wrapped in try/except), so no scope needs to exist.

## Per-user schema convention
The first code cell of every notebook:
1. Reads `current_user()`, for example `jane.doe@company.com`.
2. Takes the part before `@`, lower-cases it, and replaces any character outside `[a-z0-9_]` with `_`. For example, `jane_doe`.
3. Runs `CREATE SCHEMA IF NOT EXISTS` `` `demo-catalog`.`jane_doe` `` and sets it as the current catalog/schema.

**Every** table, view, function, volume, checkpoint and job artefact is created only in that schema (jobs are named `wb_bookings_pipeline_<user>` and `wb_payments_dq_<user>`). Nothing is written to `samples` or to shared schemas.

## Re-running & cleanup
- All notebooks are idempotent (`CREATE OR REPLACE`, `IF NOT EXISTS`, `MERGE`; 02A resets its landing files and checkpoints at the start), so **Run all** can be repeated.
- Each notebook ends with a **cleanup** cell controlled by a `cleanup` widget (default `no`). Set it to `yes` and run the cell to drop that notebook's objects (and its job, for 02A/02B).
- To remove everything for a user: `DROP SCHEMA `demo-catalog`.`<user>` CASCADE`, and delete the two jobs.

## Dataset caveats (also covered in notebook 00)
- Declared primary keys are **not unique** in `reviews`, `page_views` and `payments`. 2B uses this to make a real DQ failure.
- `page_views` / `clickstream` cover only about 2 minutes, so they are used for device/referrer/funnel analysis, not trends.
- Booking data runs from Dec 2022 to Jul 2025, and the demos treat **2025-07-31** as "today".
