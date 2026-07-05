"""
Bronze — customer CDC (Debezium) via **Auto Loader** (real Databricks execution).

Incrementally lands Debezium-style change events from a Unity Catalog Volume into
``{catalog}.bronze.customer_cdc`` as a Delta table. It keeps the raw before/after
images plus the CDC control columns Silver's SCD2 needs: ``operation_type``,
``sequence_number`` (the source LSN — true commit order), and ``event_timestamp``.

Auto Loader (``cloudFiles``) gives incremental file discovery + checkpointing;
``multiLine`` handles the JSON-array seed. In production the Debezium topic is read
directly with Structured Streaming (Kafka source, line-delimited values) — drop
``multiLine`` and point at the topic; everything downstream is identical.

Op codes map through the tested ``bronze.cdc_ingest._OP_MAP`` (single source of
truth). ``pyspark`` is imported inside ``run()`` so this file imports without Spark.
"""
from __future__ import annotations

from payments_platform.bronze import cdc_ingest  # _OP_MAP — canonical op mapping

SOURCE_SYSTEM = "customer_cdc"
# raw business fields carried in the Debezium after/before image for this feed
BUSINESS_COLUMNS = ["id", "name", "email", "phone", "country", "status"]


def default_landing_path(catalog):
    return "/Volumes/%s/landing/raw/customer_cdc" % catalog


def default_checkpoint_path(catalog):
    return "/Volumes/%s/monitoring/checkpoints/bronze_customer_cdc" % catalog


def run(catalog, run_id, landing_path=None, checkpoint_path=None):
    """Land new Debezium events into ``{catalog}.bronze.customer_cdc``.

    Uses ``availableNow`` so it drains all pending files and stops (batch-triggered
    stream) — ideal for a scheduled job. Returns the target table name."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    landing = landing_path or default_landing_path(catalog)
    checkpoint = checkpoint_path or default_checkpoint_path(catalog)
    target = "%s.bronze.customer_cdc" % catalog

    # op -> operation_type via the tested reference map (no drift)
    op_map = F.create_map(*[F.lit(x) for kv in cdc_ingest._OP_MAP.items() for x in kv])
    operation_type = F.coalesce(op_map[F.col("op")], F.lit("UNKNOWN"))

    # business payload: after-image normally, before-image for deletes
    payload = F.coalesce(F.col("after"), F.col("before"))
    # sequence = source.lsn -> source.ts_ms -> envelope ts_ms (commit order key)
    seq = F.coalesce(F.col("source.lsn"), F.col("source.ts_ms"), F.col("ts_ms")).cast("long")
    event_ts = (F.coalesce(F.col("source.ts_ms"), F.col("ts_ms")).cast("long") / F.lit(1000)).cast("timestamp")

    # record_hash over the source payload (sorted cols, "|" sep, null->"") — matches
    # common.hashing.record_hash's contract
    hash_cols = sorted(BUSINESS_COLUMNS)
    record_hash = F.sha2(F.concat_ws("|", *[
        F.coalesce(payload.getField(c).cast("string"), F.lit("")) for c in hash_cols]), 256)

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")                    # JSON-array seed (see docstring)
        .option("cloudFiles.schemaLocation", checkpoint + "/schema")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(landing)
        # flattened business columns (raw source names; Silver conforms them)
        .withColumn("id", payload.getField("id"))
        .withColumn("name", payload.getField("name"))
        .withColumn("email", payload.getField("email"))
        .withColumn("phone", payload.getField("phone"))
        .withColumn("country", payload.getField("country"))
        .withColumn("status", payload.getField("status"))
        # CDC control columns Silver's SCD2 relies on
        .withColumn("operation_type", operation_type)
        .withColumn("sequence_number", seq)
        .withColumn("event_timestamp", event_ts)
        .withColumn("_before_image", F.to_json(F.col("before")))
        .withColumn("_after_image", F.to_json(F.col("after")))
        # audit columns (mirror config.audit.BRONZE_AUDIT_COLUMNS)
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("batch_id", F.lit(run_id))
        .withColumn("record_hash", record_hash)
        .drop("op", "ts_ms", "source", "after", "before")
    )

    query = (
        stream.writeStream
        .option("checkpointLocation", checkpoint + "/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()          # availableNow: block until the batch drains
    print("bronze_customer_cdc: wrote to", target, "from", landing)
    return target
