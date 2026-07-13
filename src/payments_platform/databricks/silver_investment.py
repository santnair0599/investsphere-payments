"""
Silver — investment / treasury (real Databricks execution).

Conforms the four SQL Server treasury/asset Bronze tables into the trusted
``silver_investment`` schema, mirroring the payments conformer pattern
(:mod:`payments_platform.databricks.silver_payments`): for each table
parse/cast the raw string Bronze columns into the conformed schema, apply the
data-quality gate, route failures to the shared Failed Record Register
(``{catalog}.silver_quarantine.failed_records``), deduplicate by the primary
key (latest ``ingestion_timestamp`` wins), and **MERGE** the clean rows into
the per-table Silver Delta table.

Sources → targets (all under ``{catalog}``):
  * ``bronze.sqlserver_assets``            -> ``silver_investment.asset_clean``
  * ``bronze.sqlserver_asset_performance`` -> ``silver_investment.asset_performance_clean``
  * ``bronze.sqlserver_risk_exposure``     -> ``silver_investment.risk_exposure_clean``
  * ``bronze.sqlserver_cashflow``          -> ``silver_investment.cashflow_clean``

Allowed currency set is reused from the tested ``silver.dq`` module so the Spark
rules and the local reference share one source of truth. ``pyspark`` / ``delta``
are imported inside ``run()`` so this file imports without Spark in CI.
"""
from __future__ import annotations

from payments_platform.silver import dq

# single source of truth for currencies (no drift vs the tested payment rules)
ALLOWED_CURRENCIES = sorted(dq.ALLOWED_CURRENCIES)
ALLOWED_ASSET_CLASSES = ["EQUITY", "REAL_ESTATE", "PRIVATE_EQUITY",
                         "FIXED_INCOME", "INFRASTRUCTURE"]
ALLOWED_RISK_RATINGS = ["LOW", "MEDIUM", "HIGH"]
ALLOWED_CASHFLOW_TYPES = ["DIVIDEND", "COUPON", "CAPITAL_CALL", "DISTRIBUTION"]

# Failed Record Register columns (identical to silver_payments.QUARANTINE_COLUMNS)
QUARANTINE_COLUMNS = ["source_system", "source_table", "record_key",
                      "failure_reason", "failed_rule_name", "severity",
                      "raw_payload", "ingestion_timestamp", "failed_at",
                      "run_id", "status"]


def run(catalog, run_id):
    """Conform + DQ + quarantine + dedup + MERGE the four investment tables.

    Returns a list of ``(clean_table, quarantine_table)`` tuples. Idempotent per
    ``run_id`` and re-runnable (each MERGE upserts on the table's primary key)."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    quarantine_table = "%s.silver_quarantine.failed_records" % catalog

    def bronze(table):
        df = spark.table("%s.bronze.%s" % (catalog, table))
        if run_id:
            df = df.where(F.col("run_id") == run_id)
        return df

    results = []

    # ---- asset_clean (dimension, PK asset_id) --------------------------------
    src = bronze("sqlserver_assets")
    parsed = src.select(
        F.trim("asset_id").alias("asset_id"),
        F.trim("asset_name").alias("asset_name"),
        F.upper(F.trim("asset_class")).alias("asset_class"),
        F.trim("sector").alias("sector"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.to_date("inception_date").alias("inception_date"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("asset_id").isNull()
        | (~F.col("asset_class").isin(ALLOWED_ASSET_CLASSES))
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
    )
    results.append(_conform(spark, parsed, dq_failed, "asset_id",
                            "%s.bronze.sqlserver_assets" % catalog,
                            "asset_silver_dq",
                            "%s.silver_investment.asset_clean" % catalog,
                            quarantine_table))

    # ---- asset_performance_clean (fact, PK performance_id) -------------------
    src = bronze("sqlserver_asset_performance")
    parsed = src.select(
        F.trim("performance_id").alias("performance_id"),
        F.trim("asset_id").alias("asset_id"),
        F.to_date("as_of_date").alias("as_of_date"),
        F.col("nav").cast("decimal(18,2)").alias("nav"),
        F.col("mtd_return").cast("decimal(9,4)").alias("mtd_return"),
        F.col("ytd_return").cast("decimal(9,4)").alias("ytd_return"),
        F.col("benchmark_return").cast("decimal(9,4)").alias("benchmark_return"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("performance_id").isNull()
        | F.col("asset_id").isNull()
        | F.col("as_of_date").isNull()
        | F.col("nav").isNull()
        | (F.col("nav") < 0)
    )
    results.append(_conform(spark, parsed, dq_failed, "performance_id",
                            "%s.bronze.sqlserver_asset_performance" % catalog,
                            "asset_performance_silver_dq",
                            "%s.silver_investment.asset_performance_clean" % catalog,
                            quarantine_table))

    # ---- risk_exposure_clean (fact, PK exposure_id) --------------------------
    src = bronze("sqlserver_risk_exposure")
    parsed = src.select(
        F.trim("exposure_id").alias("exposure_id"),
        F.trim("asset_id").alias("asset_id"),
        F.to_date("as_of_date").alias("as_of_date"),
        F.col("var_95").cast("decimal(18,2)").alias("var_95"),
        F.col("volatility").cast("decimal(9,4)").alias("volatility"),
        F.col("concentration_pct").cast("decimal(9,4)").alias("concentration_pct"),
        F.upper(F.trim("risk_rating")).alias("risk_rating"),
        F.col("risk_threshold_breached").cast("boolean").alias("risk_threshold_breached"),
        # normalized raw kept only to judge castability; dropped before the clean write
        F.trim(F.col("risk_threshold_breached")).alias("risk_threshold_breached_raw"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("exposure_id").isNull()
        | F.col("asset_id").isNull()
        | F.col("as_of_date").isNull()
        | F.col("var_95").isNull()
        | (F.col("var_95") < 0)
        | (~F.col("risk_rating").isin(ALLOWED_RISK_RATINGS))
        | F.col("volatility").isNull() | (F.col("volatility") < 0) | (F.col("volatility") > 1)
        | F.col("concentration_pct").isNull()
        | (F.col("concentration_pct") < 0) | (F.col("concentration_pct") > 1)
        # castable-to-boolean: a non-blank source value that casts to NULL failed
        | (F.col("risk_threshold_breached").isNull()
           & (F.col("risk_threshold_breached_raw").isNotNull())
           & (F.col("risk_threshold_breached_raw") != F.lit("")))
    )
    results.append(_conform(spark, parsed, dq_failed, "exposure_id",
                            "%s.bronze.sqlserver_risk_exposure" % catalog,
                            "risk_exposure_silver_dq",
                            "%s.silver_investment.risk_exposure_clean" % catalog,
                            quarantine_table))

    # ---- cashflow_clean (fact, PK cashflow_id) -------------------------------
    src = bronze("sqlserver_cashflow")
    parsed = src.select(
        F.trim("cashflow_id").alias("cashflow_id"),
        F.trim("asset_id").alias("asset_id"),
        F.to_date("value_date").alias("value_date"),
        F.upper(F.trim("cashflow_type")).alias("cashflow_type"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        "source_system", "run_id", "ingestion_timestamp", "record_hash",
    )
    dq_failed = (
        F.col("cashflow_id").isNull()
        | F.col("asset_id").isNull()
        | F.col("value_date").isNull()
        | (~F.col("cashflow_type").isin(ALLOWED_CASHFLOW_TYPES))
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
    )
    results.append(_conform(spark, parsed, dq_failed, "cashflow_id",
                            "%s.bronze.sqlserver_cashflow" % catalog,
                            "cashflow_silver_dq",
                            "%s.silver_investment.cashflow_clean" % catalog,
                            quarantine_table))

    return results


def _conform(spark, parsed, dq_failed, pk, source_table, rule_name,
             clean_table, quarantine_table):
    """Quarantine DQ failures, dedup valid rows by ``pk``, MERGE into Silver."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    # quarantine failures with full context (Failed Record Register)
    quarantine = (
        parsed.where(dq_failed)
        .withColumn("source_table", F.lit(source_table))
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

    # valid rows -> dedup by pk (keep latest by ingestion_timestamp); drop any
    # helper columns (``*_raw``) used only for DQ so the Silver schema stays clean
    w = Window.partitionBy(pk).orderBy(F.col("ingestion_timestamp").desc())
    helper_cols = [c for c in parsed.columns if c.endswith("_raw")]
    clean = (parsed.where(~dq_failed)
             .withColumn("_rn", F.row_number().over(w))
             .where(F.col("_rn") == 1).drop("_rn", *helper_cols))

    # MERGE upsert into the trusted Silver table
    if spark.catalog.tableExists(clean_table):
        (DeltaTable.forName(spark, clean_table).alias("t")
            .merge(clean.alias("s"), "t.%s = s.%s" % (pk, pk))
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        clean.write.format("delta").mode("overwrite").saveAsTable(clean_table)

    print("silver_investment: merged clean -> %s ; quarantined -> %s"
          % (clean_table, quarantine_table))
    return clean_table, quarantine_table
