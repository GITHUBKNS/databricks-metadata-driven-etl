# Metadata-Driven Data Engineering Framework (Databricks + PySpark + Delta Lake)

**Status:** Starter project. Run and test in your own Databricks workspace before describing it as completed.

A small, reproducible portfolio demonstration of **configuration-driven ingestion**, source-specific validation, **Auto Loader**, Delta Lake Bronze/Silver/Gold tables, data quality quarantine, and Databricks Job orchestration. Uses entirely **synthetic customer and transaction records**.

## Architecture

See [architecture diagram](docs/architecture.md).

```text
Synthetic CSV / JSON files -> Unity Catalog volume -> Auto Loader
                               (metadata selects sources, targets and rules)
                              -> Bronze Delta tables
                              -> Silver + invalid-row quarantine
                              -> Gold transaction totals by region
                              -> Databricks SQL inspection & acceptance checks
```

## Before you start

1. Sign up for [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) or use an existing Databricks workspace.
2. Find a **Unity Catalog-enabled catalog** where you can `CREATE SCHEMA`, `CREATE VOLUME` and `CREATE TABLE`. On Free Edition, use the workspace's available catalog, which might not be `main`. Run `SHOW CATALOGS` in the SQL editor.
3. Import the `.py` files from `notebooks/` as Databricks notebooks (or open this repo as a Databricks Git folder if your workspace supports it). Attach the available serverless compute.
4. In each notebook, set the widget `catalog` to your chosen catalog; leaving it blank uses `current_catalog()`.
5. Keep this project separate from company code and data. All provided sample records are fictional.

## Run: basic version

**Step 1:** Run `notebooks/00_setup.py` once. It creates:
- `<catalog>.md_framework.pipeline_config`
- Unity Catalog managed volumes `landing` and `state`
- Two synthetic incoming files containing valid, duplicate and invalid rows

**Step 2:** Run `notebooks/01_process_source.py` twice, with the widget `pipeline_id` set to `customers` and then `transactions`. This single notebook reads source/target/format/rule settings from `pipeline_config`.

**Step 3:** Run `notebooks/02_build_gold.py`. The Gold table groups valid joined transactions by customer region. Orphan customer references are written to their own quarantine table.

**Step 4:** Run `notebooks/03_check_results.py`. It checks expected Bronze, Silver, quarantine, and Gold totals. Initial synthetic Gold total is **170.00**, and an orphan transaction worth **17.00** is separately quarantined. No arbitrary performance numbers are claimed.

## Show metadata-driven job orchestration

1. Edit `sql/read_active_pipelines.sql` to use the same catalog used in setup. Save the SELECT as a Databricks SQL query called `read_active_pipelines`.
2. Go to **Jobs & Pipelines** (sometimes shown as **Workflows**) and create a new Job.
3. Add a **SQL query** task named `read_active_pipelines` using your saved query and a SQL warehouse.
4. Add a **For each** task depending on the SQL lookup. Set **Inputs** to:

   ```text
   {{tasks.read_active_pipelines.output.rows}}
   ```

5. For the nested task, choose a **Notebook** and point it to `notebooks/01_process_source.py`. Set parameters:

   ```text
   pipeline_id = {{input.pipeline_id}}
   catalog = YOUR_CATALOG
   ```

6. Choose concurrency `2`, suitable for the two independent demo pipelines. Run the Job and inspect the task iterations. If you want a complete scheduled flow, make `02_build_gold` a downstream notebook task and `03_check_results` a final downstream task.

Official Databricks workflow tutorial: https://docs.databricks.com/aws/en/jobs/how-to/foreach-sql-lookup-tutorial

## Demonstrate a third source without editing the processor

1. Run `notebooks/04_add_partner_source.py`. It adds a new synthetic JSON feed and metadata row for **partner_transactions**. It reuses the existing `transactions` validation rule set.
2. Run `01_process_source.py` with `pipeline_id=partner_transactions` **or** rerun the metadata-driven Job. The processor code is unchanged.
3. Rerun `02_build_gold.py` and `03_check_results.py`. The new total should be **265.00** (170.00 + 35.00 + 60.00).
4. Capture a screenshot of the third control-table row, its successful Job iteration, and the updated Gold table. This is the strongest proof that configuration controls the pipeline.

## What to show in GitHub

- This `README.md` and architecture diagram
- Notebook source code and SQL
- Screenshots **from your own runs** in `docs/screenshots/` (add only after execution)
- A table of observed validation counts and records in `docs/results.md` (create once tested)
- A short explanation of design tradeoffs and future improvements

Do **not** commit access tokens, private customer files, workspace secrets, real account identifiers, or raw production exports.

## GitHub publication

After testing, create a public repo named `databricks-metadata-driven-etl`, then from the extracted folder:

```bash
git init
git add .
git commit -m "Build metadata-driven Databricks ETL demo"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/databricks-metadata-driven-etl.git
git push -u origin main
```

Create the empty GitHub repository without initializing a README to avoid an unnecessary merge. You will authenticate using the supported GitHub sign-in or credential method.

## Production improvements worth discussing in interviews

- Move Silver and Gold from snapshot `overwrite` to incremental `MERGE` and backfill-aware processing.
- Add source update timestamps and deterministic deduplication for conflicting records.
- Add rule metadata versions, schema contracts, tests, and reporting SLA definitions.
- Track per-run metrics in an audit Delta table and alert on policy failures.
- Use least-privilege permissions on the configuration, storage volumes, and target tables.
- Use deployment bundles and CI automation once the notebook version is working.

## Official references

- [Databricks Free Edition and limits](https://docs.databricks.com/aws/en/getting-started/free-edition)
- [Databricks Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [Unity Catalog volumes](https://docs.databricks.com/aws/en/volumes/)
- [Metadata-driven For each Jobs](https://docs.databricks.com/aws/en/jobs/how-to/foreach-sql-lookup-tutorial)

## Project attribution

A self-directed portfolio learning project based on the generic architecture pattern discussed in a public data-engineering Reel and official Databricks documentation. It is not an employer project or a production deployment.
