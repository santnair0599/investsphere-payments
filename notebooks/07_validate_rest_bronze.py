# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: REST API → Bronze (paginated + incremental)
# MAGIC
# MAGIC Confirms `src/payments_platform/databricks/bronze_rest_api.py` ran: the
# MAGIC configured endpoints land in Bronze Delta with their raw payload + audit
# MAGIC columns, and the incremental watermark is persisted/advanced in
# MAGIC `silver_control.ingestion_watermark` (keyed by `api_name.endpoint`).
# MAGIC
# MAGIC Prereq: the secret scope has `rest-api-token` (+ `rest-api-base-url` or a
# MAGIC `base_url` in the config); then run the `bronze_rest_api` task. Config:
# MAGIC `seeds/rest/api_config.json`.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
wm_table = f"{catalog}.silver_control.ingestion_watermark"
# (bronze table, watermark key = api_name.endpoint, watermark field)
sources = [
    ("bronze.rest_fx_rates",  "fx_rates_api./v1/fx/rates", "updated_at"),
    ("bronze.rest_merchants", "merchant_api./v1/merchants", "updated_at"),
]
print(catalog, wm_table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Bronze REST tables — row counts + audit + raw payload
# MAGIC Every row carries `source_system`, `api_name`, `endpoint`, `run_id`,
# MAGIC `ingestion_timestamp`, `source_extract_timestamp`, `record_hash`, and the
# MAGIC `_raw_response` JSON payload (schema-on-read).

# COMMAND ----------
for tbl, _, _ in sources:
    fq = f"{catalog}.{tbl}"
    if spark.catalog.tableExists(fq):
        print(f"{fq}: {spark.table(fq).count()} rows")
    else:
        print(f"{fq}: (not created — endpoint not loaded this run)")

# COMMAND ----------
display(spark.sql(f"""
  SELECT source_system, api_name, endpoint, http_status, run_id,
         ingestion_timestamp, source_extract_timestamp,
         substr(record_hash, 1, 12) AS record_hash, _raw_response
  FROM {catalog}.bronze.rest_fx_rates
  ORDER BY ingestion_timestamp DESC LIMIT 20
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ingestion watermark control table (keyed by api_name.endpoint)

# COMMAND ----------
display(spark.sql(f"""
  SELECT source_table AS watermark_key, watermark_column, watermark_value,
         load_type, run_id, updated_at
  FROM {wm_table}
  WHERE source_table LIKE '%_api.%' OR source_table LIKE '%/%'
  ORDER BY source_table
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Watermark advances **only after a successful write**
# MAGIC The module writes Bronze first, then advances the watermark from the
# MAGIC **written** rows, so `watermark_value` can never exceed the max `updated_at`
# MAGIC actually landed. A failed pull/write leaves the control row untouched and a
# MAGIC retry re-pulls from the last good watermark (no gap).

# COMMAND ----------
rows = []
for tbl, wm_key, wm_field in sources:
    fq = f"{catalog}.{tbl}"
    if not spark.catalog.tableExists(fq):
        continue
    bronze_max = spark.sql(f"SELECT CAST(MAX({wm_field}) AS string) FROM {fq}").collect()[0][0]
    wm_val = spark.sql(
        f"SELECT watermark_value FROM {wm_table} WHERE source_table = '{wm_key}'").collect()
    wm_val = wm_val[0][0] if wm_val else None
    rows.append((wm_key, wm_val, bronze_max,
                 (wm_val is None and bronze_max is None) or (wm_val == bronze_max)))

display(spark.createDataFrame(
    rows, ["watermark_key", "watermark_value", "bronze_max_updated_at", "watermark_not_ahead"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Incremental behaviour
# MAGIC Rows landed per run — re-running after new API data appends only rows newer
# MAGIC than the stored watermark (`updated_since` pushed to the API).

# COMMAND ----------
display(spark.sql(f"""
  SELECT run_id, count(*) AS rows_landed
  FROM {catalog}.bronze.rest_fx_rates
  GROUP BY run_id ORDER BY run_id DESC
"""))
