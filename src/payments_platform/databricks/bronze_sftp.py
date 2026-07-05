"""
Bronze — SFTP / vendor-file ingestion (real Databricks execution).

Reads vendor files that have landed in a Unity Catalog Volume (dropped there by the
SFTP transfer) into Bronze Delta, driven by ``seeds/sftp/file_config.json``. For
each enabled source it lists the landing path via the ``binaryFile`` source (which
gives path / size / modificationTime / content), and per file:

  * matches the configured ``file_pattern`` (extracts the ``(?P<date>...)`` group);
  * computes a SHA-256 **checksum** and skips files already in
    ``{catalog}.silver_control.processed_files`` (idempotency / duplicate detection);
  * parses the CSV (``file_ingest.parse_csv``) into clean + corrupt rows;
  * a file that parses to zero clean rows is marked **CORRUPT** (no Bronze write) so
    it doesn't break the other sources.

Bronze rows carry: ``source_system``, ``source_file_name`` / ``source_file_path``,
``file_date``, ``run_id``, ``ingestion_timestamp``, ``source_extract_timestamp`` (the
file's modified time), ``record_hash`` (+ ``_corrupt_record``, ``batch_id``). Bronze is
written first; **processed-file tracking is advanced only after that write succeeds**
(a failed write leaves the source un-tracked so a retry reprocesses it).

Reuses ``sftp_ingest`` (checksum / pattern / date / status constants) + ``file_ingest``
+ ``source_config``. ``pyspark`` imported inside ``run()`` (imports clean without Spark).
"""
from __future__ import annotations

import os

from payments_platform.bronze import file_ingest, source_config, sftp_ingest

PROCESSED_FILES_TABLE = "silver_control.processed_files"

_DEFAULT_CONFIG = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "seeds", "sftp", "file_config.json"))


def _landing_path(cfg, catalog):
    return cfg.get("landing_path") or (
        "/Volumes/%s/landing/raw/sftp/%s" % (catalog, cfg["source_system"]))


def _load_processed_checksums(spark, pf_fqn):
    if not spark.catalog.tableExists(pf_fqn):
        return set()
    return {r[0] for r in spark.table(pf_fqn).select("checksum").distinct().collect()}


def _pf_row(cfg, name, path, checksum, size, modified, file_date, status, written, run_id):
    return (cfg["source_system"], name, path, checksum, int(size), modified,
            file_date, status, int(written), run_id)


def _ingest_source(spark, catalog, cfg, run_id):
    from pyspark.sql import functions as F
    from pyspark.sql.types import (LongType, StringType, StructField, StructType,
                                    TimestampType)

    source_system = cfg["source_system"]
    pattern, business_columns = cfg["file_pattern"], cfg["business_columns"]
    landing = _landing_path(cfg, catalog)
    target = "%s.%s" % (catalog, cfg["target_bronze_table"])
    pf_fqn = "%s.%s" % (catalog, PROCESSED_FILES_TABLE)

    try:
        files_df = spark.read.format("binaryFile").load(landing)
        file_rows = files_df.select("path", "modificationTime", "length", "content").collect()
    except Exception as exc:  # noqa: BLE001 — path missing / empty landing zone
        print("bronze_sftp: %s -> no files at %s (%s)" % (source_system, landing, exc))
        return {"source": source_system, "status": "NO_FILES", "records_written": 0}

    seen = _load_processed_checksums(spark, pf_fqn)
    handled = set()
    bronze_rows, pf_rows = [], []

    for fr in file_rows:
        path = fr["path"]
        name = path.rstrip("/").rsplit("/", 1)[-1]
        modified, size, content = fr["modificationTime"], fr["length"], fr["content"]

        if not sftp_ingest.match_pattern(name, pattern):
            continue                                        # not this vendor's file
        checksum = sftp_ingest.file_checksum(content)       # SHA-256 of raw bytes
        if checksum in seen or checksum in handled:
            pf_rows.append(_pf_row(cfg, name, path, checksum, size, modified,
                                   None, sftp_ingest.DUPLICATE, 0, run_id))
            handled.add(checksum)
            continue

        file_date = sftp_ingest.extract_file_date(name, pattern)
        text = content.decode("utf-8", errors="replace") if isinstance(content, (bytes, bytearray)) else str(content)
        clean, corrupt = file_ingest.parse_csv(text, business_columns)

        if not clean and text.strip():                      # fully corrupt file
            pf_rows.append(_pf_row(cfg, name, path, checksum, size, modified,
                                   file_date, sftp_ingest.CORRUPT, 0, run_id))
            handled.add(checksum)
            continue

        for r in clean:
            row = {c: (str(r.get(c)) if r.get(c) is not None else None) for c in business_columns}
            row.update(_corrupt_record=None, source_file_name=name, source_file_path=path,
                       file_date=file_date, source_extract_timestamp=modified)
            bronze_rows.append(row)
        for bad in corrupt:                                 # keep bad rows with a marker
            row = {c: None for c in business_columns}
            row.update(_corrupt_record=bad["_raw"] + " :: " + bad["_reason"],
                       source_file_name=name, source_file_path=path,
                       file_date=file_date, source_extract_timestamp=modified)
            bronze_rows.append(row)
        pf_rows.append(_pf_row(cfg, name, path, checksum, size, modified,
                               file_date, sftp_ingest.OK, len(clean), run_id))
        handled.add(checksum)

    # ---- write Bronze FIRST; only then advance processed-file tracking --------
    fields = list(business_columns) + ["_corrupt_record", "source_file_name",
                                       "source_file_path", "file_date", "source_extract_timestamp"]
    if bronze_rows:
        schema = StructType(
            [StructField(c, StringType()) for c in business_columns] + [
                StructField("_corrupt_record", StringType()),
                StructField("source_file_name", StringType()),
                StructField("source_file_path", StringType()),
                StructField("file_date", StringType()),
                StructField("source_extract_timestamp", TimestampType())])
        tuples = [tuple(row.get(f) for f in fields) for row in bronze_rows]
        hash_expr = F.sha2(F.concat_ws("|", *[
            F.coalesce(F.col(c).cast("string"), F.lit("")) for c in sorted(business_columns)]), 256)
        bronze = (spark.createDataFrame(tuples, schema)
                  .withColumn("source_system", F.lit(source_system))
                  .withColumn("run_id", F.lit(run_id))
                  .withColumn("batch_id", F.lit(run_id))
                  .withColumn("operation_type", F.lit("INSERT"))
                  .withColumn("ingestion_timestamp", F.current_timestamp())
                  .withColumn("record_hash", hash_expr))
        (bronze.write.format("delta").mode("append")
            .option("mergeSchema", "true").saveAsTable(target))        # <-- must succeed

    # processed_files tracking (reached only if the Bronze write above succeeded)
    if pf_rows:
        pf_schema = StructType([
            StructField("source_system", StringType()), StructField("source_file_name", StringType()),
            StructField("source_file_path", StringType()), StructField("checksum", StringType()),
            StructField("file_size", LongType()), StructField("modified_time", TimestampType()),
            StructField("file_date", StringType()), StructField("status", StringType()),
            StructField("records_written", LongType()), StructField("run_id", StringType())])
        (spark.createDataFrame(pf_rows, pf_schema)
            .withColumn("processed_at", F.current_timestamp())
            .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(pf_fqn))

    ok = sum(1 for r in pf_rows if r[7] == sftp_ingest.OK)
    written = len(bronze_rows)
    print("bronze_sftp: %s -> %s files=%d ok=%d rows=%d"
          % (source_system, target, len(pf_rows), ok, written))
    return {"source": source_system, "status": "SUCCESS", "files": len(pf_rows),
            "records_written": written}


def run(catalog, run_id, config_path=None):
    """For-Each over the enabled SFTP sources. A per-source failure is logged and
    the run continues with the rest. Returns the per-source summary."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    configs = source_config.load_configs(
        config_path or _DEFAULT_CONFIG, source_config.SFTP_REQUIRED_KEYS)
    results = []
    for cfg in source_config.enabled_configs(configs):
        try:
            results.append(_ingest_source(spark, catalog, cfg, run_id))
        except Exception as exc:  # noqa: BLE001 — isolate one source; keep going
            print("bronze_sftp: FAILED %s: %s" % (cfg.get("source_system"), exc))
            results.append({"source": cfg.get("source_system"), "status": "FAILED",
                            "records_written": 0, "error": str(exc)})
    print("bronze_sftp: done —", len(results), "sources")
    return results
