"""
Bronze — marketing campaign file feed via **Auto Loader** (real Databricks execution).

Incrementally ingests campaign export CSVs from a Unity Catalog Volume (or an external
ADLS location) into ``{catalog}.bronze.campaign_file`` as a Delta table, with the same
audit columns the reference ``config.audit`` contract documents.

This is the Auto Loader lane of the enterprise DAG: ``silver_entertainment`` conforms
``bronze.campaign_file`` into ``campaign_roi_clean``, which dbt builds into
``gold_entertainment.fact_campaign_roi``. Previously nothing in the job wrote this table
-- only the synthetic demo notebook fabricated it -- so the Auto Loader source existed on
paper while ``silver_entertainment`` read a table the pipeline never produced.

Production features used: ``cloudFiles`` (Auto Loader), ``schemaLocation`` + schema
evolution, ``rescuedDataColumn``, a ``checkpointLocation``, and the ``availableNow``
trigger (batch-style incremental -- must ``awaitTermination``).

``pyspark`` is imported inside ``run()`` so this file imports without Spark in CI.
"""
from __future__ import annotations

# Business columns that make up record_hash. Sorted before hashing to match
# common.hashing.record_hash (sorted columns, "|" separator, None -> "").
BUSINESS_COLUMNS = ["campaign_id", "venue_id", "channel", "spend", "impressions",
                    "clicks", "conversions", "currency_code", "campaign_date"]

SOURCE_SYSTEM = "marketing_campaigns"


def default_landing_path(catalog):
    return "/Volumes/%s/landing/raw/campaigns" % catalog


def default_checkpoint_path(catalog):
    return "/Volumes/%s/monitoring/checkpoints/bronze_campaign" % catalog


def run(catalog, run_id, landing_path=None, checkpoint_path=None):
    """Ingest new campaign export files into ``{catalog}.bronze.campaign_file``.

    Returns the target table name. Uses ``availableNow`` so it drains all pending files
    and stops (a batch-triggered stream) -- ideal for a scheduled job.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    landing = landing_path or default_landing_path(catalog)
    checkpoint = checkpoint_path or default_checkpoint_path(catalog)
    target = "%s.bronze.campaign_file" % catalog

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
        .option("rescuedDataColumn", "_rescued_data")
        .load(landing)
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
    print("bronze_campaign_file: wrote to", target, "from", landing)
    return target
