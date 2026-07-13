"""
Silver — hospitality (real Databricks execution).

Conforms the hospitality Bronze tables (hotel master, bookings, revenue, guest
reviews) into the trusted ``{catalog}.silver_hospitality.*`` layer using the same
pattern as :mod:`silver_payments`: parse/cast → DQ gate → quarantine the failures
into the shared Failed Record Register → dedup by business key → **MERGE** the
clean rows.

One helper per table (``_conform_hotel`` / ``_conform_booking`` /
``_conform_revenue`` / ``_conform_guest_review``); each builds the conformed
projection + a boolean DQ mask and hands them to the shared ``_finish`` writer.

``ALLOWED_CURRENCIES`` is imported from the tested ``silver.dq`` module so the
Spark rules and the local reference share one source of truth. ``pyspark`` and
``delta`` are imported inside ``run()`` so this file imports without Spark in CI.
"""
from __future__ import annotations

from payments_platform.silver import dq

# single source of truth for the allowed sets (no drift vs the tested rules)
ALLOWED_CURRENCIES = sorted(dq.ALLOWED_CURRENCIES)
ALLOWED_CHANNELS = ["DIRECT", "OTA", "CORPORATE", "TRAVEL_AGENT"]
ALLOWED_BOOKING_STATUS = ["CONFIRMED", "CANCELLED", "NO_SHOW"]
ALLOWED_SENTIMENTS = ["POSITIVE", "NEUTRAL", "NEGATIVE"]

# Failed Record Register columns (mirror silver_payments.QUARANTINE_COLUMNS)
QUARANTINE_COLUMNS = ["source_system", "source_table", "record_key",
                      "failure_reason", "failed_rule_name", "severity",
                      "raw_payload", "ingestion_timestamp", "failed_at",
                      "run_id", "status"]


def run(catalog, run_id):
    """Conform + DQ + quarantine + dedup + MERGE the hospitality domain.

    Returns a dict ``{table_name: (clean_table, quarantine_table)}``. Idempotent
    per ``run_id`` and re-runnable (each MERGE upserts on its business key)."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    quarantine_table = "%s.silver_quarantine.failed_records" % catalog

    results = {}
    results["hotel_clean"] = _conform_hotel(spark, F, catalog, run_id, quarantine_table)
    results["booking_clean"] = _conform_booking(spark, F, catalog, run_id, quarantine_table)
    results["revenue_clean"] = _conform_revenue(spark, F, catalog, run_id, quarantine_table)
    results["guest_review_clean"] = _conform_guest_review(spark, F, catalog, run_id, quarantine_table)
    return results


# ---- per-table conformers ---------------------------------------------------
def _conform_hotel(spark, F, catalog, run_id, quarantine_table):
    """hotel master/dimension (PK hotel_id) from bronze.rest_hotels."""
    bronze_table = "%s.bronze.rest_hotels" % catalog
    clean_table = "%s.silver_hospitality.hotel_clean" % catalog
    bronze = _read(spark, F, bronze_table, run_id)

    parsed = bronze.select(
        F.trim("hotel_id").alias("hotel_id"),
        F.trim("hotel_name").alias("hotel_name"),
        F.initcap(F.trim("city")).alias("city"),
        F.initcap(F.trim("emirate")).alias("emirate"),
        F.col("star_rating").cast("int").alias("star_rating"),
        F.col("rooms_total").cast("int").alias("rooms_total"),
        F.trim("brand").alias("brand"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("hotel_id").isNull()
        | (F.col("star_rating") < 1) | (F.col("star_rating") > 5)
        | (F.col("rooms_total") < 0)
    )
    return _finish(spark, F, parsed, dq_failed, "hotel_id",
                   bronze_table, clean_table, quarantine_table, "hotel_silver_dq")


def _conform_booking(spark, F, catalog, run_id, quarantine_table):
    """hotel bookings (PK booking_id) from bronze.rest_hotel_bookings."""
    bronze_table = "%s.bronze.rest_hotel_bookings" % catalog
    clean_table = "%s.silver_hospitality.booking_clean" % catalog
    bronze = _read(spark, F, bronze_table, run_id)

    parsed = bronze.select(
        F.trim("booking_id").alias("booking_id"),
        F.trim("hotel_id").alias("hotel_id"),
        F.trim("customer_id").alias("customer_id"),
        F.to_date("check_in_date").alias("check_in_date"),
        F.to_date("check_out_date").alias("check_out_date"),
        F.col("room_nights").cast("int").alias("room_nights"),
        F.col("adr").cast("decimal(18,2)").alias("adr"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.upper(F.trim("channel")).alias("channel"),
        F.upper(F.trim("status")).alias("status"),
        F.to_date("booking_date").alias("booking_date"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("booking_id").isNull()
        | F.col("hotel_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("amount").isNull() | (F.col("amount") < 0)
        | (F.col("adr") < 0)
        | (F.col("room_nights") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | (~F.col("channel").isin(ALLOWED_CHANNELS))
        | (~F.col("status").isin(ALLOWED_BOOKING_STATUS))
        | F.col("check_in_date").isNull() | F.col("check_out_date").isNull()
        | (F.col("check_out_date") < F.col("check_in_date"))
    )
    return _finish(spark, F, parsed, dq_failed, "booking_id",
                   bronze_table, clean_table, quarantine_table, "booking_silver_dq")


def _conform_revenue(spark, F, catalog, run_id, quarantine_table):
    """daily hotel revenue facts (PK revenue_id) from bronze.rest_hotel_revenue."""
    bronze_table = "%s.bronze.rest_hotel_revenue" % catalog
    clean_table = "%s.silver_hospitality.revenue_clean" % catalog
    bronze = _read(spark, F, bronze_table, run_id)

    parsed = bronze.select(
        F.trim("revenue_id").alias("revenue_id"),
        F.trim("hotel_id").alias("hotel_id"),
        F.to_date("revenue_date").alias("revenue_date"),
        F.col("rooms_available").cast("int").alias("rooms_available"),
        F.col("rooms_sold").cast("int").alias("rooms_sold"),
        F.col("room_revenue").cast("decimal(18,2)").alias("room_revenue"),
        F.col("fnb_revenue").cast("decimal(18,2)").alias("fnb_revenue"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.col("revpar").cast("decimal(18,2)").alias("revpar"),
        F.col("occupancy_rate").cast("decimal(9,4)").alias("occupancy_rate"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("revenue_id").isNull()
        | F.col("hotel_id").isNull()
        | F.col("revenue_date").isNull()
        | (F.col("rooms_available") < 0)
        | (F.col("rooms_sold") < 0)
        | (F.col("room_revenue") < 0)
        | (F.col("fnb_revenue") < 0)
        | (F.col("revpar") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | (F.col("occupancy_rate") < 0) | (F.col("occupancy_rate") > 1)
    )
    return _finish(spark, F, parsed, dq_failed, "revenue_id",
                   bronze_table, clean_table, quarantine_table, "revenue_silver_dq")


def _conform_guest_review(spark, F, catalog, run_id, quarantine_table):
    """guest reviews/sentiment (PK review_id) from bronze.sfdc_case."""
    bronze_table = "%s.bronze.sfdc_case" % catalog
    clean_table = "%s.silver_hospitality.guest_review_clean" % catalog
    bronze = _read(spark, F, bronze_table, run_id)

    parsed = bronze.select(
        F.trim("review_id").alias("review_id"),
        F.trim("hotel_id").alias("hotel_id"),
        F.trim("customer_id").alias("customer_id"),
        F.to_date("review_date").alias("review_date"),
        F.col("rating").cast("int").alias("rating"),
        F.upper(F.trim("sentiment")).alias("sentiment"),
        F.trim("category").alias("category"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("review_id").isNull()
        | F.col("hotel_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("review_date").isNull()
        | F.col("rating").isNull() | (F.col("rating") < 1) | (F.col("rating") > 5)
        | (~F.col("sentiment").isin(ALLOWED_SENTIMENTS))
    )
    return _finish(spark, F, parsed, dq_failed, "review_id",
                   bronze_table, clean_table, quarantine_table, "guest_review_silver_dq")


# ---- shared helpers ---------------------------------------------------------
def _read(spark, F, bronze_table, run_id):
    """Read a Bronze table, filtered to ``run_id`` when supplied."""
    bronze = spark.table(bronze_table)
    if run_id:
        bronze = bronze.where(F.col("run_id") == run_id)
    return bronze


def _finish(spark, F, parsed, dq_failed, pk, bronze_table,
            clean_table, quarantine_table, rule_name):
    """Quarantine DQ failures, dedup the survivors by ``pk``, MERGE into Silver."""
    from pyspark.sql import Window
    from delta.tables import DeltaTable

    # quarantine failures with full context (shared Failed Record Register)
    quarantine = (
        parsed.where(dq_failed)
        .withColumn("source_table", F.lit(bronze_table))
        .withColumn("record_key", F.col(pk))
        .withColumn("failed_rule_name", F.lit(rule_name))
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

    # valid rows -> dedup by pk (keep latest by ingestion_timestamp)
    w = Window.partitionBy(pk).orderBy(F.col("ingestion_timestamp").desc())
    clean = (parsed.where(~dq_failed)
             .withColumn("_rn", F.row_number().over(w))
             .where(F.col("_rn") == 1).drop("_rn"))

    # MERGE upsert into the trusted Silver table
    if spark.catalog.tableExists(clean_table):
        (DeltaTable.forName(spark, clean_table).alias("t")
            .merge(clean.alias("s"), "t.%s = s.%s" % (pk, pk))
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        clean.write.format("delta").mode("overwrite").saveAsTable(clean_table)

    print("silver_hospitality: merged clean -> %s ; quarantined -> %s"
          % (clean_table, quarantine_table))
    return clean_table, quarantine_table
