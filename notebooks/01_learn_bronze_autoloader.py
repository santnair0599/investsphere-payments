# Databricks notebook source
# MAGIC %md
# MAGIC # Learn: Bronze ingestion with Auto Loader (payments file)
# MAGIC
# MAGIC Run this **cell by cell** in a Databricks notebook to *see* Auto Loader work.
# MAGIC It does exactly what the production module
# MAGIC `src/payments_platform/databricks/bronze_payments_autoloader.py` does — but
# MAGIC interactively, so you can inspect every step.
# MAGIC
# MAGIC Prereqs (from the walkthrough): catalog `investsphere_dev` with schemas
# MAGIC `bronze`, `landing`, `monitoring`, the Volumes `landing.raw` +
# MAGIC `monitoring.checkpoints`, and `payments_2026-06-30.csv` uploaded to
# MAGIC `/Volumes/investsphere_dev/landing/raw/payments/`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Parameters + paths
# MAGIC `spark` and `dbutils` already exist in a Databricks notebook — no SparkSession
# MAGIC to create. We just point at the landing Volume and a checkpoint location.

# COMMAND ----------
dbutils.widgets.text("catalog", "investsphere_dev")
catalog = dbutils.widgets.get("catalog")
run_id = "manual-run-01"                      # in the job this comes from the DAG

landing_path = f"/Volumes/{catalog}/landing/raw/payments"
checkpoint = f"/Volumes/{catalog}/monitoring/checkpoints/bronze_payments"
target = f"{catalog}.bronze.payments_file"

print("landing :", landing_path)
print("checkpt :", checkpoint)
print("target  :", target)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. First, look at the raw file (plain batch read)
# MAGIC This is NOT Auto Loader — it's just `spark.read` so you can see the data
# MAGIC before we ingest it. Notice the CSV columns.

# COMMAND ----------
raw = spark.read.format("csv").option("header", "true").load(landing_path)
display(raw)
print("raw row count:", raw.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Build the Auto Loader stream — this is the real ingestion
# MAGIC `cloudFiles` is Auto Loader. Each option, and *why*:
# MAGIC
# MAGIC | Option | What it does |
# MAGIC |---|---|
# MAGIC | `.format("cloudFiles")` | use **Auto Loader** (incremental file source) instead of a one-shot read |
# MAGIC | `cloudFiles.format = csv` | the underlying file format |
# MAGIC | `cloudFiles.schemaLocation` | where Auto Loader **remembers the schema** across runs (so columns are stable / can evolve) |
# MAGIC | `cloudFiles.schemaEvolutionMode = rescue` | new/unexpected data doesn't fail the stream — it's **rescued** |
# MAGIC | `rescuedDataColumn = _rescued_data` | the column that captures data that didn't fit the schema (Auto Loader's version of `_corrupt_record`) |
# MAGIC | `_metadata.file_name/path` | source-file lineage for each row |
# MAGIC
# MAGIC We also add the **audit columns** the platform's Bronze contract requires
# MAGIC (source_system, run_id, ingestion_timestamp, record_hash, …).

# COMMAND ----------
from pyspark.sql import functions as F

# record_hash over the business payload — sorted cols + "|" join to match the
# pure-Python common.hashing.record_hash contract.
business_cols = sorted(["payment_id", "customer_id", "account_id", "amount",
                        "currency", "payment_type", "transaction_date"])
hash_expr = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit(""))
                                       for c in business_cols]), 256)

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", checkpoint + "/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .load(landing_path)
    .withColumn("source_system", F.lit("payments_file"))
    .withColumn("source_file_name", F.col("_metadata.file_name"))
    .withColumn("source_file_path", F.col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("run_id", F.lit(run_id))
    .withColumn("batch_id", F.lit(run_id))
    .withColumn("operation_type", F.lit("INSERT"))
    .withColumn("record_hash", hash_expr)
)

print("is streaming DataFrame?", bronze_stream.isStreaming)   # -> True

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Write the stream to a Delta table (checkpoint + availableNow)
# MAGIC - `checkpointLocation` = where Auto Loader records **which files it already
# MAGIC   processed**. This is the incremental magic (see cell 7).
# MAGIC - `trigger(availableNow=True)` = process all files available **now**, then
# MAGIC   stop — batch-style, perfect for a scheduled job.
# MAGIC - `awaitTermination()` = wait for that batch to finish before the cell returns.
# MAGIC - `.toTable(...)` = write to a Unity Catalog **managed Delta** table.

# COMMAND ----------
query = (
    bronze_stream.writeStream
    .option("checkpointLocation", checkpoint + "/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target)
)
query.awaitTermination()
print("done — wrote to", target)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Look at what Auto Loader produced in Bronze
# MAGIC You'll see the business columns **plus** the audit columns and
# MAGIC `_rescued_data` (populated only where a row didn't fit the schema).

# COMMAND ----------
display(spark.table(target))

# COMMAND ----------
# MAGIC %sql
# MAGIC -- rows, and how many carried rescued (malformed) data
# MAGIC -- ${catalog} is substituted from the notebook widget at the top
# MAGIC SELECT count(*) AS bronze_rows,
# MAGIC        sum(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued_rows
# MAGIC FROM ${catalog}.bronze.payments_file

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Re-run to prove it's INCREMENTAL (the whole point of Auto Loader)
# MAGIC Re-run cell 4 right now **without adding a file** → it processes **0 new
# MAGIC files** (the checkpoint remembers them). Then drop a second file and re-run:
# MAGIC only the *new* file is ingested. Run the cell below to add a 2nd file, then
# MAGIC re-run cell 4, then re-run cell 5 — the row count grows only by the new file.

# COMMAND ----------
# copy the seed to a NEW filename so Auto Loader treats it as a new file
dbutils.fs.cp(
    f"{landing_path}/payments_2026-06-30.csv",
    f"{landing_path}/payments_2026-07-01.csv")
print("added a 2nd file — now re-run cell 4 (write) and cell 5 (view)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. This is exactly what the job runs — nothing is hidden
# MAGIC The production module does the same thing, wrapped in a `run()` function:
# MAGIC
# MAGIC ```python
# MAGIC from payments_platform.databricks.bronze_payments_autoloader import run
# MAGIC run(catalog="investsphere_dev", run_id="<job run id>")
# MAGIC ```
# MAGIC
# MAGIC and `pipelines/dag_task.py` calls that `run()` for the `bronze_payments_file`
# MAGIC task. So the bundle isn't magic — it's this notebook's code, packaged. Open
# MAGIC `src/payments_platform/databricks/bronze_payments_autoloader.py` and compare.
# MAGIC
# MAGIC **Next:** Silver — read this Bronze table, apply DQ, quarantine the bad rows,
# MAGIC dedup, and `MERGE` the clean rows into `silver_clean.payment_clean`.
