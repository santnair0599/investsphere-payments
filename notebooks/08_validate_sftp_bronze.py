# Databricks notebook source
# MAGIC %md
# MAGIC # Validate: SFTP vendor files → Bronze (idempotent, tracked)
# MAGIC
# MAGIC Confirms `src/payments_platform/databricks/bronze_sftp.py` ran: vendor files
# MAGIC that landed in the Volume are parsed into Bronze Delta, and every file is
# MAGIC tracked in `silver_control.processed_files` by checksum so re-runs skip it.
# MAGIC
# MAGIC Prereq: drop `seeds/sftp/settlement_2026-06-30.csv`-style files into the
# MAGIC landing Volume (default `/Volumes/<catalog>/landing/raw/sftp/card_scheme_vendor/`),
# MAGIC then run the `bronze_sftp` task. Config: `seeds/sftp/file_config.json`.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
bronze = f"{catalog}.bronze.sftp_settlements"
pf = f"{catalog}.silver_control.processed_files"
print(bronze, "|", pf)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Bronze SFTP records + audit columns
# MAGIC Each row carries `source_system`, `source_file_name` / `source_file_path`,
# MAGIC `file_date`, `run_id`, `ingestion_timestamp`, `source_extract_timestamp`
# MAGIC (the file's modified time), `record_hash`, and `_corrupt_record` for bad rows.

# COMMAND ----------
if spark.catalog.tableExists(bronze):
    print(f"{bronze}: {spark.table(bronze).count()} rows")
    display(spark.sql(f"""
      SELECT settlement_id, merchant_id, amount, currency, settlement_date,
             source_file_name, file_date, source_extract_timestamp,
             substr(record_hash, 1, 12) AS record_hash, _corrupt_record
      FROM {bronze}
      ORDER BY source_file_name, settlement_id LIMIT 30
    """))
else:
    print(f"{bronze}: (not created — no files ingested yet)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Processed-file tracking (idempotency)
# MAGIC One row per file seen, with `checksum`, `file_size`, `modified_time`,
# MAGIC `status` (SUCCESS / DUPLICATE / CORRUPT) and `records_written`.

# COMMAND ----------
display(spark.sql(f"""
  SELECT source_system, source_file_name, status, records_written,
         file_size, modified_time, file_date, substr(checksum, 1, 12) AS checksum, run_id, processed_at
  FROM {pf}
  ORDER BY processed_at DESC, source_file_name
"""))

# COMMAND ----------
# status breakdown — SUCCESS files wrote rows; DUPLICATE/CORRUPT wrote none
display(spark.sql(f"""
  SELECT status, count(*) AS files, sum(records_written) AS rows_written
  FROM {pf} GROUP BY status ORDER BY status
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Idempotency + advance-only-after-write invariants
# MAGIC - **Re-run the `bronze_sftp` task**: the same files come back as `DUPLICATE`
# MAGIC   (checksum already in `processed_files`) and write **0** new Bronze rows.
# MAGIC - Every `SUCCESS` file's `records_written` equals the rows actually in Bronze
# MAGIC   for that file (tracking is advanced only after the Bronze write succeeded).

# COMMAND ----------
# per SUCCESS file: tracked records_written vs rows actually in Bronze  (expect equal)
if spark.catalog.tableExists(bronze):
    display(spark.sql(f"""
      WITH tracked AS (
        SELECT source_file_name, records_written
        FROM {pf} WHERE status = 'SUCCESS'
      ), landed AS (
        SELECT source_file_name, count(*) AS bronze_rows
        FROM {bronze} WHERE _corrupt_record IS NULL GROUP BY source_file_name
      )
      SELECT t.source_file_name, t.records_written, l.bronze_rows,
             (t.records_written = l.bronze_rows) AS matches
      FROM tracked t LEFT JOIN landed l ON t.source_file_name = l.source_file_name
      ORDER BY t.source_file_name
    """))

# COMMAND ----------
# duplicate detection: no checksum should appear twice with status SUCCESS  (expect 0)
display(spark.sql(f"""
  SELECT checksum, count(*) AS success_rows
  FROM {pf} WHERE status = 'SUCCESS'
  GROUP BY checksum HAVING count(*) > 1
"""))
