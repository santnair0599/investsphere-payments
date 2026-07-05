"""
Silver — payments (real Databricks execution).

Reads ``{catalog}.bronze.payments_file``, conforms/casts, applies data-quality
rules, routes failures to ``{catalog}.silver_quarantine.failed_records`` (the
governance-modelled quarantine table), deduplicates by ``payment_id``, and
**MERGEs** the clean rows into ``{catalog}.silver_clean.payment_clean``.

DQ allowed-value sets are imported from the tested ``silver.dq`` module, so the
Spark rules and the local reference share one source of truth. ``pyspark`` and
``delta`` are imported inside ``run()`` so this file imports without Spark in CI.
"""
from __future__ import annotations

from payments_platform.silver import dq

# single source of truth for the allowed sets (no drift vs the tested rules)
ALLOWED_CURRENCIES = sorted(dq.ALLOWED_CURRENCIES)
ALLOWED_PAYMENT_TYPES = sorted(dq.ALLOWED_PAYMENT_TYPES)

# Failed Record Register columns (mirror silver/quarantine._register_entry)
QUARANTINE_COLUMNS = ["source_system", "source_table", "record_key",
                      "failure_reason", "failed_rule_name", "severity",
                      "raw_payload", "ingestion_timestamp", "failed_at",
                      "run_id", "status"]


def run(catalog, run_id):
    """Conform + DQ + quarantine + dedup + MERGE payments into Silver.

    Returns (clean_table, quarantine_table). Idempotent per ``run_id`` and
    re-runnable (MERGE upserts on ``payment_id``)."""
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark = SparkSession.builder.getOrCreate()
    bronze_table = "%s.bronze.payments_file" % catalog
    clean_table = "%s.silver_clean.payment_clean" % catalog
    quarantine_table = "%s.silver_quarantine.failed_records" % catalog

    bronze = spark.table(bronze_table)
    if run_id:
        bronze = bronze.where(F.col("run_id") == run_id)

    # parse / cast / standardize -> conformed Silver schema
    parsed = bronze.select(
        F.trim("payment_id").alias("payment_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("account_id").alias("account_id"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim(F.col("currency"))).alias("currency_code"),
        F.upper(F.trim(F.col("payment_type"))).alias("payment_type"),
        F.to_date("transaction_date").alias("transaction_date"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )

    # DQ rules (mirror silver/dq.PAYMENT_RULES, using the shared allowed sets)
    dq_failed = (
        F.col("payment_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("amount").isNull()
        | (F.col("amount") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | (~F.col("payment_type").isin(ALLOWED_PAYMENT_TYPES))
        | F.col("transaction_date").isNull()
    )

    # quarantine failures with full context (Failed Record Register)
    quarantine = (
        parsed.where(dq_failed)
        .withColumn("source_table", F.lit(bronze_table))
        .withColumn("record_key", F.col("payment_id"))
        .withColumn("failed_rule_name", F.lit("payment_silver_dq"))
        .withColumn("failure_reason",
                    F.lit("mandatory field / type / range / allowed-value check failed"))
        .withColumn("severity", F.lit("QUARANTINE"))
        .withColumn("raw_payload", F.to_json(F.struct("*")))
        .withColumn("failed_at", F.current_timestamp())
        .withColumn("status", F.lit("OPEN"))
        .select(*QUARANTINE_COLUMNS)
    )
    (quarantine.write.format("delta").mode("append")
        .option("mergeSchema", "true").saveAsTable(quarantine_table))

    # valid rows -> dedup by payment_id (keep latest by ingestion_timestamp)
    w = Window.partitionBy("payment_id").orderBy(F.col("ingestion_timestamp").desc())
    clean = (parsed.where(~dq_failed)
             .withColumn("_rn", F.row_number().over(w))
             .where(F.col("_rn") == 1).drop("_rn"))

    # MERGE upsert into the trusted Silver table
    if spark.catalog.tableExists(clean_table):
        (DeltaTable.forName(spark, clean_table).alias("t")
            .merge(clean.alias("s"), "t.payment_id = s.payment_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        clean.write.format("delta").mode("overwrite").saveAsTable(clean_table)

    print("silver_payments: merged clean -> %s ; quarantined -> %s"
          % (clean_table, quarantine_table))
    return clean_table, quarantine_table
