"""
Silver — customer / CRM (real Databricks execution).

Conforms the three Salesforce bronze tables into the trusted ``silver_customer``
schema, mirroring the payments conformer verbatim: parse/cast → DQ gate →
quarantine (Failed Record Register) → dedup by PK → **MERGE** into the clean
table. One helper per table, all driven from ``run(catalog, run_id)``.

Tables conformed:
  * ``bronze.sfdc_account``     → ``silver_customer.account_clean``     (PK account_id)
  * ``bronze.sfdc_contact``     → ``silver_customer.contact_clean``     (PK contact_id)
  * ``bronze.sfdc_opportunity`` → ``silver_customer.opportunity_clean`` (PK opportunity_id)

DQ allowed currencies reuse the tested ``silver.dq`` module (single source of
truth vs the local reference layer); segment/stage enums are CRM-specific.
``pyspark`` and ``delta`` are imported inside ``run()`` so this module imports
without Spark in CI.
"""
from __future__ import annotations

from payments_platform.silver import dq

# single source of truth for currencies (no drift vs the tested rules):
# {AED, USD, EUR, GBP, INR, SAR}
ALLOWED_CURRENCIES = sorted(dq.ALLOWED_CURRENCIES)

# domain enums (CRM-specific)
ALLOWED_SEGMENTS = ["RETAIL", "CORPORATE", "HNW", "TOURIST", "GOVERNMENT"]
ALLOWED_OPP_STAGES = ["PROSPECTING", "QUALIFICATION", "PROPOSAL",
                      "NEGOTIATION", "CLOSED_WON", "CLOSED_LOST"]

# Failed Record Register columns (mirror silver_payments.QUARANTINE_COLUMNS)
QUARANTINE_COLUMNS = ["source_system", "source_table", "record_key",
                      "failure_reason", "failed_rule_name", "severity",
                      "raw_payload", "ingestion_timestamp", "failed_at",
                      "run_id", "status"]

# carried through from bronze onto every conformed clean row
_LINEAGE_COLS = ["source_system", "run_id", "ingestion_timestamp", "record_hash"]


def run(catalog, run_id):
    """Conform + DQ + quarantine + dedup + MERGE the three CRM tables.

    Returns a list of ``(clean_table, quarantine_table)`` pairs. Idempotent per
    ``run_id`` and re-runnable (each MERGE upserts on the table's PK)."""
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark = SparkSession.builder.getOrCreate()
    quarantine_table = "%s.silver_quarantine.failed_records" % catalog

    ctx = (spark, F, Window, DeltaTable, catalog, run_id, quarantine_table)
    results = [
        _conform_account(*ctx),
        _conform_contact(*ctx),
        _conform_opportunity(*ctx),
    ]
    print("silver_customer: conformed %d tables -> quarantined @ %s"
          % (len(results), quarantine_table))
    return results


# ---------------------------------------------------------------------------
# shared writer: quarantine failures, dedup survivors by PK, MERGE into clean
# ``cols`` is the conformed output schema; DQ helper columns (``_*``) may live on
# ``parsed`` for the rule expressions but are dropped from both the quarantine
# payload and the clean table.
# ---------------------------------------------------------------------------
def _finish(spark, F, Window, DeltaTable, parsed, dq_failed, pk, cols,
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
        .withColumn("raw_payload", F.to_json(F.struct(*cols)))
        .withColumn("failed_at", F.current_timestamp())
        .withColumn("status", F.lit("OPEN"))
        .select(*QUARANTINE_COLUMNS)
    )
    (quarantine.write.format("delta").mode("append")
        .option("mergeSchema", "true").saveAsTable(quarantine_table))

    # valid rows -> conformed schema -> dedup by PK (keep latest by ingestion_timestamp)
    w = Window.partitionBy(pk).orderBy(F.col("ingestion_timestamp").desc())
    clean = (parsed.where(~dq_failed).select(*cols)
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

    print("silver_customer: merged clean -> %s" % clean_table)
    return clean_table, quarantine_table


def _read_bronze(spark, F, catalog, table, run_id):
    bronze_table = "%s.bronze.%s" % (catalog, table)
    bronze = spark.table(bronze_table)
    if run_id:
        bronze = bronze.where(F.col("run_id") == run_id)
    return bronze_table, bronze


# ---------------------------------------------------------------------------
# account_clean  (PK account_id)  <- bronze.sfdc_account
# ---------------------------------------------------------------------------
def _conform_account(spark, F, Window, DeltaTable, catalog, run_id, quarantine_table):
    bronze_table, bronze = _read_bronze(spark, F, catalog, "sfdc_account", run_id)

    cols = ["account_id", "account_name", "segment", "industry", "region",
            *_LINEAGE_COLS]
    parsed = bronze.select(
        F.trim("account_id").alias("account_id"),
        F.trim("account_name").alias("account_name"),
        F.upper(F.trim("segment")).alias("segment"),
        F.trim("industry").alias("industry"),
        F.trim("region").alias("region"),
        *_LINEAGE_COLS,
    )

    dq_failed = (
        F.col("account_id").isNull()
        | (~F.col("segment").isin(ALLOWED_SEGMENTS))
    )

    return _finish(spark, F, Window, DeltaTable, parsed, dq_failed, "account_id", cols,
                   bronze_table, "%s.silver_customer.account_clean" % catalog,
                   quarantine_table, "account_silver_dq")


# ---------------------------------------------------------------------------
# contact_clean  (PK contact_id)  <- bronze.sfdc_contact
# ---------------------------------------------------------------------------
def _conform_contact(spark, F, Window, DeltaTable, catalog, run_id, quarantine_table):
    bronze_table, bronze = _read_bronze(spark, F, catalog, "sfdc_contact", run_id)

    cols = ["contact_id", "account_id", "customer_id", "full_name", "email",
            "phone", "country", "is_active", *_LINEAGE_COLS]
    parsed = bronze.select(
        F.trim("contact_id").alias("contact_id"),
        F.trim("account_id").alias("account_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("full_name").alias("full_name"),
        F.lower(F.trim("email")).alias("email"),
        F.trim("phone").alias("phone"),
        F.trim("country").alias("country"),
        F.trim("is_active").alias("_is_active_raw"),
        F.col("is_active").cast("boolean").alias("is_active"),
        *_LINEAGE_COLS,
    )

    dq_failed = (
        F.col("contact_id").isNull()
        | F.col("account_id").isNull()
        # boolean must be castable: raw present but cast produced NULL -> fail
        | (F.col("_is_active_raw").isNotNull() & F.col("is_active").isNull())
    )

    return _finish(spark, F, Window, DeltaTable, parsed, dq_failed, "contact_id", cols,
                   bronze_table, "%s.silver_customer.contact_clean" % catalog,
                   quarantine_table, "contact_silver_dq")


# ---------------------------------------------------------------------------
# opportunity_clean  (PK opportunity_id)  <- bronze.sfdc_opportunity
# ---------------------------------------------------------------------------
def _conform_opportunity(spark, F, Window, DeltaTable, catalog, run_id, quarantine_table):
    bronze_table, bronze = _read_bronze(spark, F, catalog, "sfdc_opportunity", run_id)

    cols = ["opportunity_id", "account_id", "name", "stage", "amount",
            "currency_code", "close_date", "is_won", *_LINEAGE_COLS]
    parsed = bronze.select(
        F.trim("opportunity_id").alias("opportunity_id"),
        F.trim("account_id").alias("account_id"),
        F.trim("name").alias("name"),
        F.upper(F.trim("stage")).alias("stage"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.to_date("close_date").alias("close_date"),
        F.trim("is_won").alias("_is_won_raw"),
        F.col("is_won").cast("boolean").alias("is_won"),
        *_LINEAGE_COLS,
    )

    dq_failed = (
        F.col("opportunity_id").isNull()
        | F.col("account_id").isNull()
        | F.col("amount").isNull()
        | (F.col("amount") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | (~F.col("stage").isin(ALLOWED_OPP_STAGES))
        # boolean must be castable: raw present but cast produced NULL -> fail
        | (F.col("_is_won_raw").isNotNull() & F.col("is_won").isNull())
    )

    return _finish(spark, F, Window, DeltaTable, parsed, dq_failed, "opportunity_id", cols,
                   bronze_table, "%s.silver_customer.opportunity_clean" % catalog,
                   quarantine_table, "opportunity_silver_dq")
