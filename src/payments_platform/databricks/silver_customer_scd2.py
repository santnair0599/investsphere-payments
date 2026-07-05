"""
Silver — customer SCD2 (real Databricks execution).

Reads this run's Bronze CDC events, conforms them to the customer schema, and
applies them to ``{catalog}.silver_cdc.customer_scd2`` as a **Delta SCD2 MERGE**:
expire-and-insert on a real change, soft-delete on DELETE, ordered by
``sequence_number`` / event time. This is the hand-written ``foreachBatch`` + Delta
``MERGE`` form of Databricks **AUTO CDC** (``APPLY CHANGES ... SEQUENCE BY
sequence_number STORED AS SCD TYPE 2``); the logic is identical, only the engine
differs.

The change decision mirrors the tested ``silver.cdc_apply.apply_scd2`` semantics —
order by sequence, hash change-detect (no rewrite on a no-op update), soft-delete,
out-of-order/stale guard — over the shared ``CUSTOMER_TRACKED`` column set. Events
are collapsed to the **final state per key per batch** (the max-sequence event
wins; intra-batch intermediate versions are not retained — history accumulates
ACROSS batches). This is the standard SCD2-via-MERGE trade-off; an insert+delete in
one batch still yields a soft-deleted version rather than vanishing.

SCD2 columns produced: effective_from, effective_to, is_current, is_deleted,
record_hash, sequence_number. ``pyspark``/``delta`` are imported inside ``run()``.
"""
from __future__ import annotations

from payments_platform.orchestration.tasks import CUSTOMER_TRACKED  # single source of truth

KEY = "customer_id"
_UPSERTS = ["INSERT", "UPDATE", "READ"]


def run(catalog, run_id):
    """Apply this run's Bronze CDC events to the SCD2 dimension. Returns the
    target table name. Idempotent: re-running the same batch is a no-op (hashes
    unchanged, sequence not newer)."""
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark = SparkSession.builder.getOrCreate()
    bronze_table = "%s.bronze.customer_cdc" % catalog
    target = "%s.silver_cdc.customer_scd2" % catalog

    bronze = spark.table(bronze_table)
    if run_id:
        bronze = bronze.where(F.col("run_id") == run_id)

    # conform Bronze CDC -> Silver customer schema (mirrors parse.CUSTOMER_FIELD_MAP)
    conformed = (bronze.select(
        F.trim(F.col("id").cast("string")).alias("customer_id"),
        F.trim(F.col("name")).alias("customer_name"),
        F.trim(F.col("email")).alias("email"),
        F.trim(F.col("phone")).alias("phone_number"),
        F.trim(F.col("country")).alias("nationality"),
        F.upper(F.trim(F.col("status"))).alias("status"),
        F.col("operation_type"),
        F.col("sequence_number").cast("long").alias("sequence_number"),
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.col("source_system"),
    ).where(F.col("customer_id").isNotNull()))

    # record_hash over the tracked columns (sorted names, "|" sep, null->"" —
    # matches common.hashing.record_hash)
    tracked = sorted(CUSTOMER_TRACKED)
    rec_hash = F.sha2(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked]), 256)
    conformed = conformed.withColumn("record_hash", rec_hash)

    # collapse to the final event per key: the max-sequence event wins (drops
    # duplicates + stale/out-of-order lower-sequence events). _has_upsert marks a
    # key created in this batch, so an insert+delete in one batch -> soft-deleted.
    key_w = Window.partitionBy(KEY)
    latest = (conformed
              .withColumn("_rn", F.row_number().over(key_w.orderBy(F.col("sequence_number").desc())))
              .withColumn("_has_upsert",
                          F.max(F.when(F.col("operation_type").isin(_UPSERTS), 1).otherwise(0)).over(key_w))
              .where(F.col("_rn") == 1).drop("_rn"))

    is_deleted_final = (F.col("operation_type") == F.lit("DELETE"))

    def _insert_version(df):
        """Shape a change row as a new *current* SCD2 version."""
        return df.select(
            F.col(KEY),
            *[F.col(c) for c in CUSTOMER_TRACKED],
            F.col("event_timestamp").alias("effective_from"),
            F.lit(None).cast("timestamp").alias("effective_to"),
            F.lit(True).alias("is_current"),
            is_deleted_final.alias("is_deleted"),
            F.col("record_hash"),
            F.col("sequence_number"),
            F.col("source_system"),
        )

    # ---- first run: create the table from the initial current versions --------
    if not spark.catalog.tableExists(target):
        initial = latest.where(F.col("_has_upsert") == 1)   # delete-only unknown key -> skip
        (_insert_version(initial).write.format("delta")
            .option("mergeSchema", "true").saveAsTable(target))
        print("silver_customer_scd2: created", target)
        return target

    # ---- incremental: expire + insert via a union-staged SCD2 MERGE -----------
    current = (spark.table(target).where(F.col("is_current"))
               .select(F.col(KEY).alias("_k"),
                       F.col("record_hash").alias("_cur_hash"),
                       F.col("sequence_number").alias("_cur_seq"),
                       F.col("is_deleted").alias("_cur_deleted")))
    joined = latest.join(current, latest[KEY] == F.col("_k"), "left")

    new_key = F.col("_k").isNull()
    newer = F.col("sequence_number") > F.col("_cur_seq")
    changed = ((is_deleted_final & (F.col("_cur_deleted") == False)) |            # noqa: E712
               (~is_deleted_final & (F.col("record_hash") != F.col("_cur_hash"))))
    apply = joined.where((new_key & (F.col("_has_upsert") == 1)) |               # new key this batch
                         (~new_key & newer & changed))                            # existing key, real change

    # staged rows for a single MERGE:
    #   insert rows  -> _merge_key NULL (never matches) -> INSERT the new version
    #   expire rows  -> _merge_key = customer_id (matches live row) -> expire it
    inserts = (_insert_version(apply)
               .withColumn("_action", F.lit("insert"))
               .withColumn("_merge_key", F.lit(None).cast("string"))
               .withColumn("_expire_to", F.lit(None).cast("timestamp")))
    expires = (apply.where(~new_key).select(
        F.lit(None).cast("string").alias(KEY),
        *[F.lit(None).cast("string").alias(c) for c in CUSTOMER_TRACKED],
        F.lit(None).cast("timestamp").alias("effective_from"),
        F.lit(None).cast("timestamp").alias("effective_to"),
        F.lit(None).cast("boolean").alias("is_current"),
        F.lit(None).cast("boolean").alias("is_deleted"),
        F.lit(None).cast("string").alias("record_hash"),
        F.lit(None).cast("long").alias("sequence_number"),
        F.lit(None).cast("string").alias("source_system"),
        F.lit("expire").alias("_action"),
        F.col(KEY).alias("_merge_key"),
        F.col("event_timestamp").alias("_expire_to"),
    ))
    staged = inserts.unionByName(expires)

    insert_values = {KEY: "s.%s" % KEY, "record_hash": "s.record_hash",
                     "effective_from": "s.effective_from", "effective_to": "s.effective_to",
                     "is_current": "s.is_current", "is_deleted": "s.is_deleted",
                     "sequence_number": "s.sequence_number", "source_system": "s.source_system"}
    insert_values.update({c: "s.%s" % c for c in CUSTOMER_TRACKED})

    (DeltaTable.forName(spark, target).alias("t")
        .merge(staged.alias("s"), "t.%s = s._merge_key AND t.is_current = true" % KEY)
        .whenMatchedUpdate(condition="s._action = 'expire'",
                           set={"is_current": "false", "effective_to": "s._expire_to"})
        .whenNotMatchedInsert(condition="s._action = 'insert'", values=insert_values)
        .execute())

    print("silver_customer_scd2: SCD2 MERGE applied ->", target)
    return target
