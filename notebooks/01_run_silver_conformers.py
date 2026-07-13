# Databricks notebook source
# MAGIC %md
# MAGIC # Run the Silver conformers (synthetic demo path)
# MAGIC
# MAGIC Run this **after** `00_generate_synthetic_enterprise_data.py`. It calls each
# MAGIC domain Silver conformer directly — **no real Oracle / Salesforce / SQL Server /
# MAGIC SFTP / REST credentials needed**, because Bronze was already produced by the
# MAGIC generator notebook.
# MAGIC
# MAGIC Each conformer does parse → DQ gate → quarantine (`silver_quarantine.failed_records`)
# MAGIC → dedup → Delta MERGE into `silver_<domain>.*_clean`.
# MAGIC
# MAGIC > **IMPORTANT — `run_id` must match the generator.** The conformers filter Bronze
# MAGIC > by `run_id`. Use the **same** `run_id` you used in notebook 00 (`demo_run_001`),
# MAGIC > or Silver will read **zero rows**.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
dbutils.widgets.text("run_id", "demo_run_001")
catalog = dbutils.widgets.get("catalog")
run_id = dbutils.widgets.get("run_id")
print(f"catalog={catalog}  run_id={run_id}")

# COMMAND ----------
# Make the src package importable on the driver (same shim the Job entrypoint uses).
import os, sys
_REPO = os.path.dirname(os.getcwd())  # notebooks/ -> repo root; adjust if needed
for cand in (_REPO, os.path.join(_REPO, "src"), "/Workspace/Repos", None):
    if cand and os.path.isdir(os.path.join(cand, "payments_platform")):
        sys.path.insert(0, cand); break
    if cand and os.path.isdir(os.path.join(cand, "src", "payments_platform")):
        sys.path.insert(0, os.path.join(cand, "src")); break
# Fallback: if running from a Repo, the package is usually already importable.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run the five domain conformers
# MAGIC `silver_customer_scd2` is **skipped** — notebook 00 already seeded
# MAGIC `silver_cdc.customer_scd2` directly (the shared guest/customer SCD2 dimension).

# COMMAND ----------
from payments_platform.databricks import (
    silver_realestate, silver_hospitality, silver_entertainment,
    silver_investment, silver_customer,
)

CONFORMERS = [
    ("real_estate",   silver_realestate),
    ("hospitality",   silver_hospitality),
    ("entertainment", silver_entertainment),
    ("investment",    silver_investment),
    ("customer",      silver_customer),
]

for name, module in CONFORMERS:
    print(f"\n=== conforming {name} ===")
    module.run(catalog=catalog, run_id=run_id)

print("\nAll Silver conformers complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quick row-count sanity (Silver + quarantine)

# COMMAND ----------
checks = [
    "silver_realestate.property_clean", "silver_realestate.occupancy_clean",
    "silver_hospitality.booking_clean", "silver_hospitality.revenue_clean",
    "silver_entertainment.ticket_sales_clean", "silver_entertainment.footfall_clean",
    "silver_investment.asset_performance_clean", "silver_investment.risk_exposure_clean",
    "silver_customer.account_clean",
    "silver_quarantine.failed_records",
]
for t in checks:
    try:
        n = spark.table(f"{catalog}.{t}").count()
        print(f"{t:<48} rows={n}")
    except Exception as exc:
        print(f"{t:<48} MISSING/ERROR: {exc}")

# COMMAND ----------
# MAGIC %md
# MAGIC **Next:** `dbt build` then `dbt test` (see `RUNBOOK.md`), then run
# MAGIC `02_verify_enterprise_demo` to check the Gold marts + trust score.
