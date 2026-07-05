# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: Salesforce → Bronze (incremental + soft-deletes)
# MAGIC
# MAGIC Confirms `src/payments_platform/databricks/bronze_salesforce.py` ran: the
# MAGIC configured objects land in Bronze Delta with their raw object + audit columns,
# MAGIC soft-deletes are captured (`operation_type = DELETE`), and the incremental
# MAGIC watermark is persisted/advanced in `silver_control.ingestion_watermark`.
# MAGIC
# MAGIC Prereq: the secret scope has `salesforce-client-id` / `-client-secret` /
# MAGIC `-username` / `-password` (password + security token) — or a
# MAGIC `salesforce-access-token` + `salesforce-instance-url`. Then run the
# MAGIC `bronze_salesforce` task. Config: `seeds/salesforce/object_config.json`.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
wm_table = f"{catalog}.silver_control.ingestion_watermark"
# (bronze table, object, watermark/modstamp column)
objects = [
    ("bronze.sfdc_account",     "Account",     "SystemModstamp"),
    ("bronze.sfdc_contact",     "Contact",     "LastModifiedDate"),
    ("bronze.sfdc_opportunity", "Opportunity", "SystemModstamp"),
]
print(catalog, wm_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Bronze Salesforce objects — row counts + audit columns
# MAGIC Each row carries `source_system`, `source_object`, `run_id`, `batch_id`,
# MAGIC `ingestion_timestamp`, `source_extract_timestamp`, `record_hash`,
# MAGIC `operation_type` (UPSERT / DELETE) and the `_raw_object` JSON.

# COMMAND ----------
for tbl, _, _ in objects:
    fq = f"{catalog}.{tbl}"
    if spark.catalog.tableExists(fq):
        print(f"{fq}: {spark.table(fq).count()} rows")
    else:
        print(f"{fq}: (not created — object not loaded this run)")

# COMMAND ----------
display(spark.sql(f"""
  SELECT Id, source_system, source_object, load_type, operation_type, run_id,
         ingestion_timestamp, source_extract_timestamp,
         substr(record_hash, 1, 12) AS record_hash, _raw_object
  FROM {catalog}.bronze.sfdc_account
  ORDER BY ingestion_timestamp DESC LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Soft-deletes captured (queryAll)
# MAGIC `IsDeleted = true` records come through with `operation_type = DELETE` (not dropped).

# COMMAND ----------
display(spark.sql(f"""
  SELECT source_object, operation_type, count(*) AS records
  FROM {catalog}.bronze.sfdc_account
  GROUP BY source_object, operation_type ORDER BY operation_type
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Ingestion watermark (keyed by object) — advances only after write
# MAGIC `watermark_value` is the max modstamp of the rows actually landed, so it can
# MAGIC never exceed the Bronze max. A failed object leaves its control row untouched.

# COMMAND ----------
display(spark.sql(f"""
  SELECT source_table AS object, watermark_column, watermark_value, load_type, run_id, updated_at
  FROM {wm_table}
  WHERE source_table IN ('Account', 'Contact', 'Opportunity')
  ORDER BY source_table
"""))

# COMMAND ----------
rows = []
for tbl, obj, modstamp in objects:
    fq = f"{catalog}.{tbl}"
    if not spark.catalog.tableExists(fq):
        continue
    bronze_max = spark.sql(f"SELECT CAST(MAX({modstamp}) AS string) FROM {fq}").collect()[0][0]
    wm_val = spark.sql(
        f"SELECT watermark_value FROM {wm_table} WHERE source_table = '{obj}'").collect()
    wm_val = wm_val[0][0] if wm_val else None
    rows.append((obj, wm_val, bronze_max,
                 (wm_val is None and bronze_max is None) or (wm_val == bronze_max)))

display(spark.createDataFrame(
    rows, ["object", "watermark_value", "bronze_max_modstamp", "watermark_not_ahead"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Incremental behaviour
# MAGIC Re-running after new/updated Salesforce records appends only rows with
# MAGIC `modstamp > watermark` (the predicate is pushed into the SOQL `WHERE`).

# COMMAND ----------
display(spark.sql(f"""
  SELECT run_id, load_type, count(*) AS rows_landed
  FROM {catalog}.bronze.sfdc_account
  GROUP BY run_id, load_type ORDER BY run_id DESC
"""))
