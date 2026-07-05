# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: Bronze validation gate + Silver DQ gate
# MAGIC
# MAGIC Mirrors the checks in `src/payments_platform/databricks/gates.py` so you can
# MAGIC confirm each gate's verdict. The real gates run as the `bronze_validation_gate`
# MAGIC / `silver_dq_gate` tasks and publish task values
# MAGIC (`gate_status` / `gate_passed` / `records_checked` / `failed_checks` /
# MAGIC `quarantine_rate_pct` / `message`); a condition task branches on `gate_passed`.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
dbutils.widgets.text("run_id", "manual-run-01")
catalog = dbutils.widgets.get("catalog")
run_id = dbutils.widgets.get("run_id")
print(catalog, run_id)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze gate — row counts per source (required sources must be > 0)

# COMMAND ----------
targets = [
    ("payments_file", "bronze.payments_file"), ("customer_cdc", "bronze.customer_cdc"),
    ("oracle_customers", "bronze.oracle_customers"), ("oracle_transactions", "bronze.oracle_transactions"),
    ("sqlserver_accounts", "bronze.sqlserver_accounts"), ("rest_fx_rates", "bronze.rest_fx_rates"),
    ("rest_merchants", "bronze.rest_merchants"), ("sftp_settlements", "bronze.sftp_settlements"),
    ("sfdc_account", "bronze.sfdc_account"), ("sfdc_contact", "bronze.sfdc_contact"),
    ("sfdc_opportunity", "bronze.sfdc_opportunity"),
]
rows = []
for key, tbl in targets:
    fq = f"{catalog}.{tbl}"
    n = spark.table(fq).count() if spark.catalog.tableExists(fq) else None
    rows.append((tbl, n))
display(spark.createDataFrame(rows, ["bronze_table", "row_count"]))

# COMMAND ----------
# required audit columns present on every existing Bronze table (expect missing = [])
required_audit = ["source_system", "run_id", "ingestion_timestamp", "record_hash"]
audit = []
for _, tbl in targets:
    fq = f"{catalog}.{tbl}"
    if spark.catalog.tableExists(fq):
        cols = set(spark.table(fq).columns)
        audit.append((tbl, str([a for a in required_audit if a not in cols])))
display(spark.createDataFrame(audit, ["bronze_table", "missing_audit_columns"]))

# COMMAND ----------
# no unexpected corrupt-file status; watermark not ahead of the extracted max
pf = f"{catalog}.silver_control.processed_files"
if spark.catalog.tableExists(pf):
    display(spark.sql(f"SELECT status, count(*) AS files FROM {pf} GROUP BY status ORDER BY status"))

wm = f"{catalog}.silver_control.ingestion_watermark"
if spark.catalog.tableExists(wm):
    display(spark.sql(f"SELECT source_table, watermark_column, watermark_value FROM {wm} ORDER BY source_table"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver DQ gate — quarantine rate, duplicates, nulls, SCD2 validity
# MAGIC All the counts below should be **0** (except quarantine_rate which must be
# MAGIC under the threshold, default 30%).

# COMMAND ----------
clean = f"{catalog}.silver_clean.payment_clean"
quar = f"{catalog}.silver_quarantine.failed_records"
scd2 = f"{catalog}.silver_cdc.customer_scd2"

checks = []
if spark.catalog.tableExists(clean):
    c = spark.table(clean).count()
    q = spark.table(quar).count() if spark.catalog.tableExists(quar) else 0
    rate = round(q / (c + q) * 100, 2) if (c + q) else 0.0
    checks.append(("quarantine_rate_pct", rate, "< 30"))
    checks.append(("duplicate_payment_ids",
                   spark.sql(f"SELECT count(*) FROM (SELECT payment_id FROM {clean} GROUP BY payment_id HAVING count(*)>1)").collect()[0][0], "0"))
    checks.append(("null_critical_payment_cols",
                   spark.sql(f"SELECT count(*) FROM {clean} WHERE payment_id IS NULL OR customer_id IS NULL OR amount IS NULL").collect()[0][0], "0"))
if spark.catalog.tableExists(scd2):
    q1 = spark.sql(f"SELECT count(*) FROM (SELECT customer_id FROM {scd2} WHERE is_current GROUP BY customer_id HAVING count(*)>1)").collect()[0][0]
    q2 = spark.sql(f"SELECT count(*) FROM {scd2} WHERE effective_to IS NOT NULL AND effective_to < effective_from").collect()[0][0]
    q3 = spark.sql(f"SELECT count(*) FROM {scd2} WHERE is_current = true AND effective_to IS NOT NULL").collect()[0][0]
    q4 = spark.sql(f"SELECT count(*) FROM (SELECT sequence_number, LAG(sequence_number) OVER (PARTITION BY customer_id ORDER BY effective_from) AS prev FROM {scd2}) WHERE prev IS NOT NULL AND sequence_number < prev").collect()[0][0]
    checks += [("scd2_one_current_per_customer", q1, "0"),
               ("scd2_effective_from_le_to", q2, "0"),
               ("scd2_current_row_open", q3, "0"),
               ("scd2_sequence_forward_moving", q4, "0")]
display(spark.createDataFrame(checks, ["check", "value", "expected"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run the real gates and show the published task-value payload
# MAGIC (Optional — needs `payments_platform` on the path, e.g. bundle-synced repo.)

# COMMAND ----------
import os, sys
for p in ("/Workspace/Repos", os.getcwd()):
    src = os.path.join(p, "src")
    if os.path.isdir(src) and src not in sys.path:
        sys.path.insert(0, src)
try:
    from payments_platform.databricks.gates import bronze_gate, silver_dq_gate
    b = bronze_gate(spark, catalog, run_id)
    s = silver_dq_gate(spark, catalog, run_id)
    print("BRONZE:", b["gate_status"], "| passed=", b["gate_passed"], "| checked=", b["records_checked"])
    print("  failed:", b["failed_checks"])
    print("SILVER:", s["gate_status"], "| passed=", s["gate_passed"], "| quar_rate=", s["quarantine_rate_pct"])
    print("  failed:", s["failed_checks"])
    display(spark.createDataFrame(
        [(c["name"], c["passed"], c["detail"]) for c in b["checks"] + s["checks"]],
        ["check", "passed", "detail"]))
except Exception as exc:
    print("gate module not importable here (%s) — the SQL cells above cover the same checks" % exc)
