"""
Bronze — metadata-driven JDBC ingestion (real Databricks execution).

Loads Oracle / SQL Server tables into Bronze Delta via **Spark JDBC**, driven by
the same source-config the reference ``bronze.jdbc_ingest`` uses. For each enabled
table:

  * **full** load        -> read the whole table, overwrite the Bronze snapshot;
  * **incremental** load -> read only rows with ``watermark_col > last_watermark``
    (the predicate is pushed down to the source DB as a sub-query), append to Bronze;
  * **first run** of an incremental table (no stored watermark) -> full seed load.

The watermark is persisted/advanced in the Delta control table
``{catalog}.silver_control.ingestion_watermark`` and only moves forward, after a
successful read. Credentials come from the Key Vault-backed secret scope
(``{source_system}-jdbc-url`` / ``-jdbc-user`` / ``-jdbc-password``) via
``dbutils.secrets`` — never embedded here.

Reuses the reference ``source_config`` loader + ``jdbc_ingest`` audit/constants so
the Spark path and the local reference agree. ``pyspark`` is imported inside
``run()`` so this file imports without Spark in CI.
"""
from __future__ import annotations

import os

from payments_platform.bronze import jdbc_ingest, source_config

SECRET_SCOPE = "investsphere_payments"          # KV-backed scope (Terraform)
WATERMARK_TABLE = "silver_control.ingestion_watermark"
DEFAULT_FETCHSIZE = 10000                        # JDBC rows fetched per round-trip

# JDBC driver per source system (override with a `jdbc_driver` config key)
_DRIVERS = {
    "oracle": "oracle.jdbc.OracleDriver",
    "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


def _watermark_predicate(source_system, wm_col, value):
    """Build a dialect-correct ``watermark_col > <literal>`` predicate.

    Oracle needs an explicit ``TO_DATE``/``TO_TIMESTAMP`` (no implicit string->date
    cast for a bind literal); SQL Server takes a ``CAST(... AS DATETIME2)``; anything
    else falls back to a quoted literal. The stored watermark is Spark's
    ``cast(<col> as string)`` form (``yyyy-MM-dd[ HH:mm:ss]``)."""
    ss = (source_system or "").lower()
    v = str(value).split(".")[0]                 # drop fractional seconds
    if ss == "oracle":
        lit = ("TO_DATE('%s', 'YYYY-MM-DD')" % v if len(v) == 10
               else "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS')" % v)
    elif ss == "sqlserver":
        lit = "CAST('%s' AS DATETIME2)" % value
    else:
        lit = "'%s'" % value
    return "%s > %s" % (wm_col, lit)


def _read_options(cfg):
    """Per-table JDBC read tuning: fetchsize, and optional bounded parallel read
    (partitionColumn/lowerBound/upperBound/numPartitions) for large tables. Spark
    requires all four partition options together, so they apply only if a
    partition_column + num_partitions + both bounds are configured."""
    opts = {"fetchsize": str(cfg.get("fetchsize", DEFAULT_FETCHSIZE))}
    pcol, npart = cfg.get("partition_column"), cfg.get("num_partitions")
    lb, ub = cfg.get("lower_bound"), cfg.get("upper_bound")
    if pcol and npart and lb is not None and ub is not None:
        opts.update({"partitionColumn": pcol, "numPartitions": str(npart),
                     "lowerBound": str(lb), "upperBound": str(ub)})
    return opts

_DEFAULT_CONFIG = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "seeds", "jdbc", "source_config.json"))


def _connection(source_system, cfg):
    """Resolve JDBC url/user/password/driver. url/user/password come from the
    secret scope; the driver defaults per source system."""
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    dbutils = DBUtils(SparkSession.builder.getOrCreate())

    def secret(suffix):
        return dbutils.secrets.get(scope=SECRET_SCOPE, key="%s-jdbc-%s" % (source_system, suffix))

    driver = cfg.get("jdbc_driver") or _DRIVERS.get(source_system)
    if driver is None:
        raise ValueError("no JDBC driver for source_system=%s (set jdbc_driver)" % source_system)
    return {"url": secret("url"), "user": secret("user"),
            "password": secret("password"), "driver": driver}


def _dbtable(cfg, last_watermark):
    """Full read -> the table; incremental -> a predicate-pushdown sub-query with a
    dialect-correct watermark literal."""
    table, wm_col = cfg["source_table"], cfg.get("watermark_column")
    if cfg["load_type"] == jdbc_ingest.INCREMENTAL and last_watermark is not None:
        # the source DB evaluates the WHERE (predicate pushdown)
        pred = _watermark_predicate(cfg["source_system"], wm_col, last_watermark)
        return "(SELECT * FROM %s WHERE %s) AS _t" % (table, pred), "incremental"
    if cfg["load_type"] == jdbc_ingest.INCREMENTAL:
        return table, "full_seed"          # first incremental run = seed the table
    return table, "full"


def _read_watermark(spark, wm_fqn, source_table):
    from pyspark.sql import functions as F
    if not spark.catalog.tableExists(wm_fqn):
        return None
    rows = (spark.table(wm_fqn)
            .where(F.col("source_table") == source_table)
            .select("watermark_value").collect())
    return rows[0][0] if rows else None


def _upsert_watermark(spark, wm_fqn, values):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    cols = ["source_table", "watermark_column", "watermark_value", "load_type", "run_id"]
    df = spark.createDataFrame([values], cols).withColumn("updated_at", F.current_timestamp())
    if spark.catalog.tableExists(wm_fqn):
        (DeltaTable.forName(spark, wm_fqn).alias("t")
            .merge(df.alias("s"), "t.source_table = s.source_table")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        df.write.format("delta").saveAsTable(wm_fqn)


def _ingest_one(spark, catalog, cfg, run_id):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    source_table = cfg["source_table"]
    wm_col = cfg.get("watermark_column")
    target = "%s.%s" % (catalog, cfg["target_bronze_table"])   # e.g. bronze.oracle_customers
    wm_fqn = "%s.%s" % (catalog, WATERMARK_TABLE)
    pk = cfg["primary_key"] if isinstance(cfg["primary_key"], list) else [cfg["primary_key"]]

    last_wm = _read_watermark(spark, wm_fqn, source_table)
    dbtable, mode = _dbtable(cfg, last_wm)
    conn = _connection(cfg["source_system"], cfg)

    df = (spark.read.format("jdbc")
          .option("url", conn["url"]).option("user", conn["user"])
          .option("password", conn["password"]).option("driver", conn["driver"])
          .option("dbtable", dbtable)
          .options(**_read_options(cfg))            # fetchsize + optional partitioning
          .load())

    business_cols = df.columns
    # dedup by primary key -> keep latest per key (by watermark if present)
    if wm_col and wm_col in business_cols:
        df = (df.withColumn("_rn", F.row_number().over(
                Window.partitionBy(*pk).orderBy(F.col(wm_col).desc())))
              .where(F.col("_rn") == 1).drop("_rn"))
    else:
        df = df.dropDuplicates(pk)

    # audit columns (mirror jdbc_ingest.JDBC_AUDIT_COLUMNS) + record_hash
    hash_expr = F.sha2(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in sorted(business_cols)]), 256)
    bronze = (df
              .withColumn("source_system", F.lit(cfg["source_system"]))
              .withColumn("source_table", F.lit(source_table))
              .withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("batch_id", F.lit(run_id))
              .withColumn("run_id", F.lit(run_id))
              .withColumn("source_extract_timestamp", F.current_timestamp())
              .withColumn("record_hash", hash_expr))

    write_mode = "overwrite" if mode == "full" else "append"
    (bronze.write.format("delta").mode(write_mode)
        .option("mergeSchema", "true").saveAsTable(target))     # <-- must succeed first

    # Advance the watermark ONLY after the write succeeded, and derive it from the
    # WRITTEN Bronze data (never ahead of what actually landed). If the write above
    # raised, we never reach here -> the control row is untouched and a retry
    # re-reads from the last good watermark (no skipped rows).
    landed = spark.table(target).where(F.col("run_id") == run_id)
    written = landed.count()
    new_wm = last_wm
    if wm_col and wm_col in business_cols and written > 0:
        new_wm = spark.table(target).agg(F.max(F.col(wm_col)).cast("string")).collect()[0][0]
        _upsert_watermark(spark, wm_fqn, (source_table, wm_col, new_wm, cfg["load_type"], run_id))

    print("bronze_jdbc: %s -> %s [%s] rows=%d watermark=%s"
          % (source_table, target, mode, written, new_wm))
    return {"source_table": source_table, "mode": mode, "records_written": written,
            "watermark": new_wm}


def run(catalog, run_id, config_path=None):
    """For-Each over the enabled JDBC source configs. A per-table failure is
    logged and the run continues with the rest. Returns the per-table summary."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    configs = source_config.load_source_config(config_path or _DEFAULT_CONFIG)
    results = []
    for cfg in source_config.enabled_configs(configs):
        try:
            results.append(_ingest_one(spark, catalog, cfg, run_id))
        except Exception as exc:  # noqa: BLE001 — isolate one table; keep going
            print("bronze_jdbc: FAILED %s: %s" % (cfg.get("source_table"), exc))
            results.append({"source_table": cfg.get("source_table"),
                            "mode": "FAILED", "records_written": 0, "error": str(exc)})
    print("bronze_jdbc: done —", len(results), "tables")
    return results
