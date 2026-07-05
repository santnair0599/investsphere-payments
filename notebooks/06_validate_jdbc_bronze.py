# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: JDBC → Bronze (incremental + watermark)
# MAGIC
# MAGIC Confirms `src/payments_platform/databricks/bronze_jdbc.py` ran: the
# MAGIC configured source tables land in Bronze Delta, and the incremental
# MAGIC watermark is persisted/advanced in `silver_control.ingestion_watermark`.
# MAGIC
# MAGIC Prereq: the source DB is reachable and the secret scope has
# MAGIC `<source_system>-jdbc-url` / `-jdbc-user` / `-jdbc-password`; then run the
# MAGIC `bronze_jdbc` task (or the job). Config: `seeds/jdbc/source_config.json`.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
wm_table = f"{catalog}.silver_control.ingestion_watermark"
# Bronze targets from the enabled configs (oracle customers/transactions incr, sqlserver accounts full)
bronze_tables = ["bronze.oracle_customers", "bronze.oracle_transactions", "bronze.sqlserver_accounts"]
print(catalog, wm_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Bronze JDBC tables — row counts + audit columns
# MAGIC Every row carries `source_system`, `source_table`, `run_id`, `record_hash`,
# MAGIC `ingestion_timestamp`, `source_extract_timestamp`.

# COMMAND ----------
for t in bronze_tables:
    fq = f"{catalog}.{t}"
    if spark.catalog.tableExists(fq):
        n = spark.table(fq).count()
        print(f"{fq}: {n} rows")
    else:
        print(f"{fq}: (not created — table not loaded this run)")

# COMMAND ----------
# sample one table with its audit columns
display(spark.sql(f"""
  SELECT * FROM {catalog}.bronze.oracle_customers
  ORDER BY ingestion_timestamp DESC LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ingestion watermark control table
# MAGIC One row per source table; `watermark_value` is the max of the configured
# MAGIC watermark column seen so far. Full-load tables (e.g. `accounts`) also track it.

# COMMAND ----------
display(spark.sql(f"""
  SELECT source_table, watermark_column, watermark_value, load_type, run_id, updated_at
  FROM {wm_table}
  ORDER BY source_table
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Incremental behaviour
# MAGIC Re-running `bronze_jdbc` after new/updated source rows should:
# MAGIC - read only rows with `watermark_col > watermark_value` (predicate pushed to the DB),
# MAGIC - **append** them to the Bronze table,
# MAGIC - **advance** `watermark_value` forward.
# MAGIC
# MAGIC The query below shows rows landed by the most recent run (`run_id`).

# COMMAND ----------
display(spark.sql(f"""
  SELECT run_id, source_table, count(*) AS rows_landed
  FROM {catalog}.bronze.oracle_customers
  GROUP BY run_id, source_table
  ORDER BY run_id DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Watermark advances **only after a successful write**
# MAGIC The module writes Bronze first, then advances the watermark — and derives it
# MAGIC from the **written** rows. So the stored `watermark_value` can never be ahead
# MAGIC of the max watermark actually landed in Bronze. If a write fails, the control
# MAGIC row is untouched and a retry re-reads from the last good watermark (no gap).
# MAGIC
# MAGIC Invariant below: `watermark_value == max(watermark_col in Bronze)` for each
# MAGIC loaded table, and `watermark_not_ahead = true` everywhere.

# COMMAND ----------
# (source_table, bronze_table, watermark_col) for the loaded sources
checks = [
    ("customers",    "bronze.oracle_customers",    "last_updated_date"),
    ("transactions", "bronze.oracle_transactions", "last_updated_date"),
    ("accounts",     "bronze.sqlserver_accounts",  "modified_at"),
]
rows = []
for src, tbl, wm in checks:
    fq = f"{catalog}.{tbl}"
    if not spark.catalog.tableExists(fq):
        continue
    bronze_max = spark.sql(f"SELECT CAST(MAX({wm}) AS string) FROM {fq}").collect()[0][0]
    wm_val = spark.sql(
        f"SELECT watermark_value FROM {wm_table} WHERE source_table = '{src}'"
    ).collect()
    wm_val = wm_val[0][0] if wm_val else None
    rows.append((src, wm_val, bronze_max,
                 (wm_val is None and bronze_max is None) or (wm_val == bronze_max)))

display(spark.createDataFrame(
    rows, ["source_table", "watermark_value", "bronze_max_watermark", "watermark_not_ahead"]))

# COMMAND ----------
# watermark control row is stamped at (or after) the Bronze write for that run — a
# failed write would leave updated_at trailing the missing Bronze rows.
display(spark.sql(f"""
  SELECT w.source_table, w.watermark_value, w.updated_at AS watermark_updated_at,
         b.last_bronze_write
  FROM {wm_table} w
  LEFT JOIN (
      SELECT source_table, max(ingestion_timestamp) AS last_bronze_write
      FROM {catalog}.bronze.oracle_customers GROUP BY source_table
  ) b ON w.source_table = b.source_table
  ORDER BY w.source_table
"""))

