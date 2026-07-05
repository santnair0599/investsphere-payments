# Azure Databricks implementation — step by step

A follow-along runbook to take this repo from local reference to a **running Azure
Databricks pipeline**, staged. This milestone does the **file-based path only**,
end to end:

```
payments CSV in a UC Volume
   → Auto Loader → investsphere_dev.bronze.payments_file
   → Spark Silver (DQ/quarantine/dedup/MERGE) → investsphere_dev.silver_clean.payment_clean
   → dbt (MERGE) → investsphere_dev.gold.fact_payments
```

The other sources (JDBC/CDC/REST/SFTP/Salesforce) stay as stubs and are added
later — see [What's next](#whats-next).

## What's already done in the repo ✓

You do **not** need to write the Spark code — it's implemented and tested:

- `src/payments_platform/databricks/bronze_payments_autoloader.py` — real Auto Loader.
- `src/payments_platform/databricks/silver_payments.py` — parse → DQ → quarantine
  (`silver_quarantine.failed_records`) → dedup → **Delta MERGE** into
  `silver_clean.payment_clean`.
- `pipelines/dag_task.py` — dispatches every task to a real module: all six Bronze
  sources, `silver_payments`, `silver_customer_scd2`, and both validation gates.
- `databricks.yml` — passes `--catalog` to each task; dbt builds the full Gold graph.
- `dbt/` — `sources.yml` + `dbt_project.yml` read/write the catalog from a var.

The whole `daily_e2e` job runs on real Spark end to end (this runbook uses the file
path as the simplest starting point; land the other sources' inputs / secrets to
light them up — see the source-specific ingest docs).

## Phase 0 — Azure prerequisites (one-time, in Azure)

- Azure subscription; `az login`.
- **Azure Databricks workspace** (Premium — required for Unity Catalog).
- A **Unity Catalog metastore** (one per region) assigned to the workspace.
- **Entra ID** groups (`data_engineers`, `analysts`, `pii_approved_users`,
  `data_stewards`, `spn_investsphere_etl`) synced via SCIM. For a dev-only run you
  can skip most of these — dev runs as **you** (the catalog owner).
- A storage account/container for **Terraform remote state**.
- Databricks CLI authenticated: set `DATABRICKS_HOST` / `DATABRICKS_TOKEN` (or a
  CLI profile) for the workspace.

> Keep it cheap: a small **serverless** SQL warehouse with auto-terminate.

## Phase 1 — Terraform (dev only)

Fill `infra/terraform/envs/dev/terraform.tfvars.json` (subscription/tenant/
`databricks_host`/`databricks_account_id`/names), then:

```bash
terraform -chdir=infra/terraform/envs/dev init -backend-config=backend.hcl
terraform -chdir=infra/terraform/envs/dev plan
terraform -chdir=infra/terraform/envs/dev apply
terraform -chdir=infra/terraform/envs/dev output -raw warehouse_id   # copy this
```

This creates the catalog `investsphere_dev`, **all schemas** (bronze, silver_*,
gold, gold_marts, monitoring, …), the SQL warehouse, secret scope, cluster policy,
and grants. **Do not create test/prod yet.**

## Phase 2 — Fill `bundle_vars/dev.yml`

```yaml
catalog: investsphere_dev
secret_scope: investsphere_payments
etl_service_principal: spn_investsphere_etl
warehouse_id: "<paste terraform output -raw warehouse_id>"
```

## Phase 3 — Landing Volume + a couple of extra schemas + upload the file

Run in a Databricks SQL editor / notebook (as the catalog owner):

```sql
-- landing + checkpoints as UC Volumes (simplest; abfss:// external location works too)
CREATE SCHEMA IF NOT EXISTS investsphere_dev.landing;
CREATE VOLUME  IF NOT EXISTS investsphere_dev.landing.raw;
CREATE VOLUME  IF NOT EXISTS investsphere_dev.monitoring.checkpoints;

-- dbt writes its staging views here; not in the Terraform schema set, so create it
CREATE SCHEMA IF NOT EXISTS investsphere_dev.gold_staging;
```

Upload the seed file to the Volume path the Auto Loader watches:

```
seeds/payments/payments_2026-06-30.csv
   →  /Volumes/investsphere_dev/landing/raw/payments/payments_2026-06-30.csv
```

(UI: Catalog → investsphere_dev → landing → raw → create folder `payments` →
Upload; or `databricks fs cp seeds/payments/payments_2026-06-30.csv dbfs:/Volumes/investsphere_dev/landing/raw/payments/`.)

## Phase 4 — Deploy + run + verify

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run investsphere_payments_daily_e2e -t dev
```

Then verify the medallion actually materialised:

```sql
SELECT count(*), min(ingestion_timestamp) FROM investsphere_dev.bronze.payments_file;
SELECT count(*) FROM investsphere_dev.silver_clean.payment_clean;          -- valid, deduped
SELECT * FROM investsphere_dev.silver_quarantine.failed_records LIMIT 20;   -- bad rows w/ context
SELECT * FROM investsphere_dev.gold.fact_payments LIMIT 20;                 -- dbt MERGE output
```

You should see the quarantine table capture the invalid rows (negative amount /
bad currency / missing key) and `payment_clean` hold the valid, de-duplicated set.

### If serverless streaming complains

Auto Loader on serverless is fine for `availableNow` directory listing, but if the
`bronze_payments_file` task errors on the stream, run those two Spark tasks on a
small **job cluster** instead of serverless. In `databricks.yml`, replace
`environment_key: default` on `bronze_payments_file` + `silver_payments` with a
`job_cluster_key`, and add a `job_clusters:` entry (single-node, latest LTS,
autotermination) governed by the Terraform cluster policy.

### Library on the cluster

Out of the box `dag_task.py` puts `./src` on `sys.path` and the bundle syncs the
repo, so imports resolve. To do it the production-correct way, uncomment the wheel
`artifacts` + serverless `environments.dependencies` block at the top of
`databricks.yml` (`pip install build` first).

## Idempotency / re-runs

- Bronze Auto Loader is checkpointed — re-running only ingests **new** files.
- Silver **MERGE**es on `payment_id`, so re-runs upsert (no duplicates).
- dbt `fact_payments` is incremental (`unique_key='payment_id'`) → **MERGE**.

To reprocess from scratch in dev: `DROP` the three tables + delete the checkpoint
Volume folder, re-upload the file, re-run.

## What's next

Add each remaining source the same way — a real module under
`databricks/`, wired into `dag_task.py`, with `--catalog` on its bundle task:

1. **Customer CDC → SCD2** — `spark.readStream` from Kafka (or a CDC feed table) →
   `silver_customer_scd2` via **AUTO CDC** (`apply_changes ... SEQUENCE BY ...
   STORED AS SCD TYPE 2`) or `foreachBatch` + MERGE. Then **widen the dbt selector**
   back to `staging gold gold_marts` (dim_customer + daily_payment_summary need it).
2. **JDBC** (Oracle/SQL Server) — Spark JDBC read with the watermark predicate.
3. **REST / SFTP / Salesforce** — Spark equivalents of the reference ingestors.
4. **Monitoring/control rows** — write `silver_control.*` / `monitoring.*` from the
   tasks (the `RunMonitor` contract).
5. **Real gates** — replace the stubbed gate task values with actual checks over
   `silver_control.table_load_status`.
6. **Power BI** — DirectQuery to the warehouse as the `analysts` group.

Promote to **test** then **prod** only after dev is solid — same repo, different
tfvars + `bundle_vars`, prod gated behind approval (see
[DATABRICKS_DEPLOYMENT.md](DATABRICKS_DEPLOYMENT.md)).
