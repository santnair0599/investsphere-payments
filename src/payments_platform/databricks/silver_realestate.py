"""
Silver — real estate (real Databricks execution).

Conforms the four Oracle PMS bronze tables into the trusted ``silver_realestate``
schema, mirroring the payments conformer verbatim: parse/cast → DQ gate →
quarantine (Failed Record Register) → dedup by PK → **MERGE** into the clean
table. One helper per table, all driven from ``run(catalog, run_id)``.

Tables conformed:
  * ``bronze.oracle_properties``        → ``silver_realestate.property_clean``
  * ``bronze.oracle_leases``            → ``silver_realestate.lease_clean``
  * ``bronze.oracle_occupancy_daily``   → ``silver_realestate.occupancy_clean``
  * ``bronze.oracle_maintenance_orders``→ ``silver_realestate.maintenance_clean``

DQ allowed-value sets reuse the tested ``silver.dq`` module where possible, so
the Spark rules share one source of truth with the local reference layer.
``pyspark`` and ``delta`` are imported inside ``run()`` so this module imports
without Spark in CI.
"""
from __future__ import annotations

from payments_platform.silver import dq

# single source of truth for currencies (no drift vs the tested rules)
ALLOWED_CURRENCIES = sorted(dq.ALLOWED_CURRENCIES)

# domain enums (real-estate specific)
ALLOWED_PROPERTY_TYPES = ["RESIDENTIAL", "COMMERCIAL", "RETAIL", "HOSPITALITY", "MIXED_USE"]
ALLOWED_LEASE_STATUS = ["ACTIVE", "EXPIRED", "TERMINATED"]
ALLOWED_MAINT_PRIORITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
ALLOWED_MAINT_STATUS = ["OPEN", "IN_PROGRESS", "CLOSED"]

# Failed Record Register columns (mirror silver_payments.QUARANTINE_COLUMNS)
QUARANTINE_COLUMNS = ["source_system", "source_table", "record_key",
                      "failure_reason", "failed_rule_name", "severity",
                      "raw_payload", "ingestion_timestamp", "failed_at",
                      "run_id", "status"]

# carried through from bronze onto every conformed clean row
_LINEAGE_COLS = ["source_system", "run_id", "ingestion_timestamp", "record_hash"]


def run(catalog, run_id):
    """Conform + DQ + quarantine + dedup + MERGE the four real-estate tables.

    Returns a list of ``(clean_table, quarantine_table)`` pairs. Idempotent per
    ``run_id`` and re-runnable (each MERGE upserts on the table's PK)."""
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark = SparkSession.builder.getOrCreate()
    quarantine_table = "%s.silver_quarantine.failed_records" % catalog

    ctx = (spark, F, Window, DeltaTable, catalog, run_id, quarantine_table)
    results = [
        _conform_property(*ctx),
        _conform_lease(*ctx),
        _conform_occupancy(*ctx),
        _conform_maintenance(*ctx),
    ]
    print("silver_realestate: conformed %d tables -> quarantined @ %s"
          % (len(results), quarantine_table))
    return results


# ---------------------------------------------------------------------------
# shared writer: quarantine failures, dedup survivors by PK, MERGE into clean
# ---------------------------------------------------------------------------
def _finish(spark, F, Window, DeltaTable, parsed, dq_failed, pk,
            bronze_table, clean_table, quarantine_table, rule_name):
    # quarantine failures with full context (Failed Record Register)
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

    # valid rows -> dedup by PK (keep latest by ingestion_timestamp)
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

    print("silver_realestate: merged clean -> %s" % clean_table)
    return clean_table, quarantine_table


def _read_bronze(spark, F, catalog, table, run_id):
    bronze_table = "%s.bronze.%s" % (catalog, table)
    bronze = spark.table(bronze_table)
    if run_id:
        bronze = bronze.where(F.col("run_id") == run_id)
    return bronze_table, bronze


# ---------------------------------------------------------------------------
# property_clean  (PK property_id)  <- bronze.oracle_properties
# ---------------------------------------------------------------------------
def _conform_property(spark, F, Window, DeltaTable, catalog, run_id, quarantine_table):
    bronze_table, bronze = _read_bronze(spark, F, catalog, "oracle_properties", run_id)

    parsed = bronze.select(
        F.trim("property_id").alias("property_id"),
        F.trim("property_name").alias("property_name"),
        F.upper(F.trim("property_type")).alias("property_type"),
        F.trim("city").alias("city"),
        F.trim("emirate").alias("emirate"),
        F.col("gross_leasable_area_sqm").cast("decimal(18,2)").alias("gross_leasable_area_sqm"),
        F.col("units_total").cast("int").alias("units_total"),
        F.to_date("acquisition_date").alias("acquisition_date"),
        *_LINEAGE_COLS,
    )

    dq_failed = (
        F.col("property_id").isNull()
        | (~F.col("property_type").isin(ALLOWED_PROPERTY_TYPES))
        | (F.col("gross_leasable_area_sqm") < 0)
        | (F.col("units_total") < 0)
    )

    return _finish(spark, F, Window, DeltaTable, parsed, dq_failed, "property_id",
                   bronze_table, "%s.silver_realestate.property_clean" % catalog,
                   quarantine_table, "property_silver_dq")


# ---------------------------------------------------------------------------
# lease_clean  (PK lease_id)  <- bronze.oracle_leases
# ---------------------------------------------------------------------------
def _conform_lease(spark, F, Window, DeltaTable, catalog, run_id, quarantine_table):
    bronze_table, bronze = _read_bronze(spark, F, catalog, "oracle_leases", run_id)

    parsed = bronze.select(
        F.trim("lease_id").alias("lease_id"),
        F.trim("property_id").alias("property_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("unit_id").alias("unit_id"),
        F.col("monthly_rent").cast("decimal(18,2)").alias("monthly_rent"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.to_date("lease_start").alias("lease_start"),
        F.to_date("lease_end").alias("lease_end"),
        F.upper(F.trim("status")).alias("status"),
        *_LINEAGE_COLS,
    )

    dq_failed = (
        F.col("lease_id").isNull()
        | F.col("property_id").isNull()
        | F.col("customer_id").isNull()
        | F.col("monthly_rent").isNull()
        | (F.col("monthly_rent") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | (~F.col("status").isin(ALLOWED_LEASE_STATUS))
    )

    return _finish(spark, F, Window, DeltaTable, parsed, dq_failed, "lease_id",
                   bronze_table, "%s.silver_realestate.lease_clean" % catalog,
                   quarantine_table, "lease_silver_dq")


# ---------------------------------------------------------------------------
# occupancy_clean  (PK occupancy_id)  <- bronze.oracle_occupancy_daily
# ---------------------------------------------------------------------------
def _conform_occupancy(spark, F, Window, DeltaTable, catalog, run_id, quarantine_table):
    bronze_table, bronze = _read_bronze(spark, F, catalog, "oracle_occupancy_daily", run_id)

    parsed = bronze.select(
        F.trim("occupancy_id").alias("occupancy_id"),
        F.trim("property_id").alias("property_id"),
        F.to_date("occupancy_date").alias("occupancy_date"),
        F.col("units_occupied").cast("int").alias("units_occupied"),
        F.col("units_total").cast("int").alias("units_total"),
        F.col("occupancy_rate").cast("decimal(9,4)").alias("occupancy_rate"),
        *_LINEAGE_COLS,
    )

    dq_failed = (
        F.col("occupancy_id").isNull()
        | F.col("property_id").isNull()
        | F.col("occupancy_date").isNull()
        | (F.col("units_occupied") < 0)
        | (F.col("units_total") < 0)
        | (F.col("occupancy_rate") < 0)
        | (F.col("occupancy_rate") > 1)
    )

    return _finish(spark, F, Window, DeltaTable, parsed, dq_failed, "occupancy_id",
                   bronze_table, "%s.silver_realestate.occupancy_clean" % catalog,
                   quarantine_table, "occupancy_silver_dq")


# ---------------------------------------------------------------------------
# maintenance_clean  (PK work_order_id)  <- bronze.oracle_maintenance_orders
# ---------------------------------------------------------------------------
def _conform_maintenance(spark, F, Window, DeltaTable, catalog, run_id, quarantine_table):
    bronze_table, bronze = _read_bronze(spark, F, catalog, "oracle_maintenance_orders", run_id)

    parsed = bronze.select(
        F.trim("work_order_id").alias("work_order_id"),
        F.trim("property_id").alias("property_id"),
        F.trim("category").alias("category"),
        F.upper(F.trim("priority")).alias("priority"),
        F.col("cost").cast("decimal(18,2)").alias("cost"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.to_date("opened_date").alias("opened_date"),
        F.to_date("closed_date").alias("closed_date"),
        F.upper(F.trim("status")).alias("status"),
        *_LINEAGE_COLS,
    )

    dq_failed = (
        F.col("work_order_id").isNull()
        | F.col("property_id").isNull()
        | F.col("cost").isNull()
        | (F.col("cost") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | (~F.col("priority").isin(ALLOWED_MAINT_PRIORITIES))
        | (~F.col("status").isin(ALLOWED_MAINT_STATUS))
    )

    return _finish(spark, F, Window, DeltaTable, parsed, dq_failed, "work_order_id",
                   bronze_table, "%s.silver_realestate.maintenance_clean" % catalog,
                   quarantine_table, "maintenance_silver_dq")
