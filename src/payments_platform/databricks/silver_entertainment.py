"""
Silver — entertainment (real Databricks execution).

Conforms the four entertainment Bronze tables into the trusted
``{catalog}.silver_entertainment.*`` schema, mirroring the payments Silver
pattern verbatim: parse/cast → **DQ gate** → **quarantine (Failed Record
Register)** → dedup → **MERGE**.

Sources (Bronze contract: raw business columns as strings + ``source_system,
run_id, ingestion_timestamp, record_hash``):
  - ``bronze.entertainment_venues``  → ``venue_clean``        (PK venue_id)
  - ``bronze.sftp_ticket_sales``     → ``ticket_sales_clean`` (PK ticket_id)
  - ``bronze.sftp_footfall``         → ``footfall_clean``     (PK footfall_id)
  - ``bronze.campaign_file``         → ``campaign_roi_clean`` (PK campaign_id+campaign_date)

Allowed currencies and the QUARANTINE_COLUMNS register are imported from the
tested payments modules so there is one source of truth (no drift). ``pyspark``
and ``delta`` are imported inside ``run()`` so this file imports without Spark.
"""
from __future__ import annotations

from payments_platform.silver import dq
from payments_platform.databricks.silver_payments import QUARANTINE_COLUMNS

# single source of truth for currencies (no drift vs the tested rules)
ALLOWED_CURRENCIES = sorted(dq.ALLOWED_CURRENCIES)

# domain enums
ALLOWED_VENUE_TYPES = ["THEME_PARK", "CINEMA", "ARENA", "MALL_ATTRACTION", "WATERPARK"]
ALLOWED_CHANNELS = ["SOCIAL", "SEARCH", "EMAIL", "OOH", "INFLUENCER"]


def run(catalog, run_id):
    """Conform + DQ + quarantine + dedup + MERGE the four entertainment tables.

    Returns a dict of ``{logical_name: clean_table}``. Idempotent per ``run_id``
    and re-runnable (each table MERGEs on its business key)."""
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark = SparkSession.builder.getOrCreate()
    quarantine_table = "%s.silver_quarantine.failed_records" % catalog

    def _bronze(table):
        b = spark.table("%s.bronze.%s" % (catalog, table))
        if run_id:
            b = b.where(F.col("run_id") == run_id)
        return b

    def _quarantine(parsed, dq_failed, source_table, record_key_col, rule_name):
        bad = (
            parsed.where(dq_failed)
            .withColumn("source_table", F.lit("%s.bronze.%s" % (catalog, source_table)))
            .withColumn("record_key", F.col(record_key_col).cast("string"))
            .withColumn("failed_rule_name", F.lit(rule_name))
            .withColumn("failure_reason",
                        F.lit("mandatory field / type / range / allowed-value check failed"))
            .withColumn("severity", F.lit("QUARANTINE"))
            .withColumn("raw_payload", F.to_json(F.struct("*")))
            .withColumn("failed_at", F.current_timestamp())
            .withColumn("status", F.lit("OPEN"))
            .select(*QUARANTINE_COLUMNS)
        )
        (bad.write.format("delta").mode("append")
            .option("mergeSchema", "true").saveAsTable(quarantine_table))

    def _merge(clean, clean_table, key_cols):
        """Dedup by ``key_cols`` (keep latest by ingestion_timestamp) then MERGE."""
        w = Window.partitionBy(*key_cols).orderBy(F.col("ingestion_timestamp").desc())
        deduped = (clean.withColumn("_rn", F.row_number().over(w))
                   .where(F.col("_rn") == 1).drop("_rn"))
        if spark.catalog.tableExists(clean_table):
            cond = " AND ".join("t.%s = s.%s" % (c, c) for c in key_cols)
            (DeltaTable.forName(spark, clean_table).alias("t")
                .merge(deduped.alias("s"), cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        else:
            deduped.write.format("delta").mode("overwrite").saveAsTable(clean_table)
        return clean_table

    _AUDIT = ["source_system", "run_id", "ingestion_timestamp", "record_hash"]
    results = {}

    # ---- venue_clean (dimension) ------------------------------------------
    venue = _bronze("entertainment_venues").select(
        F.trim("venue_id").alias("venue_id"),
        F.trim("venue_name").alias("venue_name"),
        F.upper(F.trim("venue_type")).alias("venue_type"),
        F.trim("city").alias("city"),
        F.trim("emirate").alias("emirate"),
        F.col("capacity").cast("int").alias("capacity"),
        *_AUDIT,
    )
    venue_failed = (
        F.col("venue_id").isNull()
        | F.col("venue_name").isNull()
        | (~F.col("venue_type").isin(ALLOWED_VENUE_TYPES))
        | F.col("capacity").isNull()
        | (F.col("capacity") < 0)
    )
    _quarantine(venue, venue_failed, "entertainment_venues", "venue_id", "venue_silver_dq")
    results["venue_clean"] = _merge(
        venue.where(~venue_failed), "%s.silver_entertainment.venue_clean" % catalog, ["venue_id"])

    # ---- ticket_sales_clean (fact) ----------------------------------------
    tickets = _bronze("sftp_ticket_sales").select(
        F.trim("ticket_id").alias("ticket_id"),
        F.trim("venue_id").alias("venue_id"),
        F.trim("event_id").alias("event_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.to_date("sale_date").alias("sale_date"),
        *_AUDIT,
    )
    tickets_failed = (
        F.col("ticket_id").isNull()
        | F.col("venue_id").isNull()
        | F.col("quantity").isNull()
        | (F.col("quantity") < 0)
        | F.col("amount").isNull()
        | (F.col("amount") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | F.col("sale_date").isNull()
    )
    _quarantine(tickets, tickets_failed, "sftp_ticket_sales", "ticket_id", "ticket_sales_silver_dq")
    results["ticket_sales_clean"] = _merge(
        tickets.where(~tickets_failed), "%s.silver_entertainment.ticket_sales_clean" % catalog,
        ["ticket_id"])

    # ---- footfall_clean (fact) --------------------------------------------
    footfall = _bronze("sftp_footfall").select(
        F.trim("footfall_id").alias("footfall_id"),
        F.trim("venue_id").alias("venue_id"),
        F.trim("gate").alias("gate"),
        F.col("visitors").cast("int").alias("visitors"),
        F.to_date("footfall_date").alias("footfall_date"),
        *_AUDIT,
    )
    footfall_failed = (
        F.col("footfall_id").isNull()
        | F.col("venue_id").isNull()
        | F.col("visitors").isNull()
        | (F.col("visitors") < 0)
        | F.col("footfall_date").isNull()
    )
    _quarantine(footfall, footfall_failed, "sftp_footfall", "footfall_id", "footfall_silver_dq")
    results["footfall_clean"] = _merge(
        footfall.where(~footfall_failed), "%s.silver_entertainment.footfall_clean" % catalog,
        ["footfall_id"])

    # ---- campaign_roi_clean (fact; composite PK campaign_id + campaign_date) --
    campaign = _bronze("campaign_file").select(
        F.trim("campaign_id").alias("campaign_id"),
        F.trim("venue_id").alias("venue_id"),
        F.upper(F.trim("channel")).alias("channel"),
        F.col("spend").cast("decimal(18,2)").alias("spend"),
        F.col("impressions").cast("int").alias("impressions"),
        F.col("clicks").cast("int").alias("clicks"),
        F.col("conversions").cast("int").alias("conversions"),
        F.upper(F.trim("currency_code")).alias("currency_code"),
        F.to_date("campaign_date").alias("campaign_date"),
        *_AUDIT,
    )
    campaign_failed = (
        F.col("campaign_id").isNull()
        | F.col("venue_id").isNull()
        | F.col("campaign_date").isNull()
        | (~F.col("channel").isin(ALLOWED_CHANNELS))
        | F.col("spend").isNull()
        | (F.col("spend") < 0)
        | (F.col("impressions") < 0)
        | (F.col("clicks") < 0)
        | (F.col("conversions") < 0)
        | (~F.col("currency_code").isin(ALLOWED_CURRENCIES))
        | (F.col("clicks") > F.col("impressions"))
        | (F.col("conversions") > F.col("clicks"))
    )
    _quarantine(campaign, campaign_failed, "campaign_file", "campaign_id", "campaign_roi_silver_dq")
    # dedup + MERGE on the composite business key (a campaign has one row per date)
    results["campaign_roi_clean"] = _merge(
        campaign.where(~campaign_failed), "%s.silver_entertainment.campaign_roi_clean" % catalog,
        ["campaign_id", "campaign_date"])

    print("silver_entertainment: merged -> %s ; quarantined -> %s"
          % (list(results.values()), quarantine_table))
    return results
