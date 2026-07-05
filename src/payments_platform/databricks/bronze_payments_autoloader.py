"""
Bronze — payments file feed via **Auto Loader** (real Databricks execution).

Incrementally ingests new CSVs from a Unity Catalog Volume (or an external ADLS
location) into ``{catalog}.bronze.payments_file`` as a Delta table, adding the
same audit columns the reference ``config.audit`` contract documents — so Silver
and dbt see identical columns whether the pipeline runs locally or on Databricks.

Production features used: ``cloudFiles`` (Auto Loader), ``schemaLocation`` +
schema evolution, ``rescuedDataColumn`` (Auto Loader's equivalent of the
reference ``_corrupt_record``), a ``checkpointLocation``, and the
``availableNow`` trigger (batch-style incremental — must ``awaitTermination``).

``pyspark`` is imported inside ``run()`` so this file imports without Spark in CI.
"""
from __future__ import annotations

# Business columns that make up record_hash. Sorted before hashing to match
# common.hashing.record_hash (sorted columns, "|" separator, None -> "").
BUSINESS_COLUMNS = ["payment_id", "customer_id", "account_id", "amount",
                    "currency", "payment_type", "transaction_date"]

SOURCE_SYSTEM = "payments_file"


def default_landing_path(catalog):
    return "/Volumes/%s/landing/raw/payments" % catalog


def default_checkpoint_path(catalog):
    return "/Volumes/%s/monitoring/checkpoints/bronze_payments" % catalog


def run(catalog, run_id, landing_path=None, checkpoint_path=None):
    """Ingest new payment files into ``{catalog}.bronze.payments_file``.

    Returns the target table name. Uses ``availableNow`` so it drains all pending
    files and stops (a batch-triggered stream) — ideal for a scheduled job.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    landing = landing_path or default_landing_path(catalog)
    checkpoint = checkpoint_path or default_checkpoint_path(catalog)
    target = "%s.bronze.payments_file" % catalog

    # record_hash over the business payload, matching the Python contract
    hash_cols = sorted(BUSINESS_COLUMNS)
    hash_expr = F.sha2(
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit(""))
                           for c in hash_cols]), 256)

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", checkpoint + "/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")   # == reference _corrupt_record
        .load(landing)
        # audit columns (mirror config.audit.BRONZE_AUDIT_COLUMNS)
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("source_file_name", F.col("_metadata.file_name"))
        .withColumn("source_file_path", F.col("_metadata.file_path"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("batch_id", F.lit(run_id))
        .withColumn("operation_type", F.lit("INSERT"))
        .withColumn("record_hash", hash_expr)
    )

    query = (
        stream.writeStream
        .option("checkpointLocation", checkpoint + "/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()      # availableNow: block until the batch drains
    print("bronze_payments_file: wrote to", target, "from", landing)
    return target
