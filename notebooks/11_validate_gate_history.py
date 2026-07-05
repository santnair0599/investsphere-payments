# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: gate results persisted to control tables
# MAGIC
# MAGIC The `bronze_validation_gate` / `silver_dq_gate` tasks write their outcome to
# MAGIC Delta control tables (in addition to publishing task values for orchestration):
# MAGIC - `silver_control.pipeline_run_audit` — one summary row per gate run
# MAGIC - `silver_control.dq_results` — one row per individual check
# MAGIC - `silver_control.table_load_status` — one row per table the gate observed
# MAGIC
# MAGIC Run the gate tasks (or the daily job), then run this notebook.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
audit = f"{catalog}.silver_control.pipeline_run_audit"
dq = f"{catalog}.silver_control.dq_results"
tls = f"{catalog}.silver_control.table_load_status"
print(audit, dq, tls)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Latest run status (per gate)
# MAGIC The most recent audit row for each gate — status, pass/fail, rows checked.

# COMMAND ----------
display(spark.sql(f"""
  WITH ranked AS (
    SELECT *, row_number() OVER (PARTITION BY gate_name ORDER BY check_timestamp DESC) AS rn
    FROM {audit}
  )
  SELECT run_id, task_name, gate_name, gate_status, gate_passed,
         records_checked, quarantine_rate_pct, message, check_timestamp
  FROM ranked WHERE rn = 1
  ORDER BY gate_name
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Failed checks (latest run)
# MAGIC From `dq_results` — every individual check that did not pass.

# COMMAND ----------
display(spark.sql(f"""
  SELECT run_id, task_name, gate_name, check_name, passed, detail, check_timestamp
  FROM {dq}
  WHERE passed = false
  ORDER BY check_timestamp DESC, gate_name, check_name
"""))

# COMMAND ----------
# per-gate check summary (passed vs failed) for the latest run of each gate
display(spark.sql(f"""
  SELECT gate_name, passed, count(*) AS checks
  FROM {dq}
  WHERE run_id = (SELECT run_id FROM {audit} ORDER BY check_timestamp DESC LIMIT 1)
  GROUP BY gate_name, passed ORDER BY gate_name, passed
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Quarantine rate (from the Silver DQ gate)

# COMMAND ----------
display(spark.sql(f"""
  SELECT run_id, quarantine_rate_pct, gate_status, check_timestamp
  FROM {audit}
  WHERE gate_name = 'silver_dq'
  ORDER BY check_timestamp DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Gate history (all runs)
# MAGIC Pass/fail trend over time — useful for a Databricks SQL dashboard.

# COMMAND ----------
display(spark.sql(f"""
  SELECT check_timestamp, run_id, gate_name, gate_status, gate_passed,
         records_checked, quarantine_rate_pct,
         CASE WHEN failed_checks = '' THEN NULL ELSE failed_checks END AS failed_checks
  FROM {audit}
  ORDER BY check_timestamp DESC
  LIMIT 100
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Table load status observed by the gate (latest run)

# COMMAND ----------
display(spark.sql(f"""
  SELECT task_name, table_name, source_key, records, status, check_timestamp
  FROM {tls}
  WHERE run_id = (SELECT run_id FROM {audit} ORDER BY check_timestamp DESC LIMIT 1)
  ORDER BY task_name, table_name
"""))
