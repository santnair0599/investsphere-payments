"""
Real medallion quality gates (Databricks Spark).

``bronze_gate`` and ``silver_dq_gate`` run actual checks over the Bronze/Silver
Delta tables + control tables and return a structured result. ``pipelines/dag_task.py``
publishes that result as task values (``gate_status`` / ``gate_passed`` /
``records_checked`` / ``failed_checks`` / ``quarantine_rate_pct`` / ``message``) and a
**condition task** branches on ``gate_passed`` — pass runs downstream, fail blocks it.

Thresholds reuse the tested reference policies in ``orchestration.gates``. The Bronze
target list + watermark keys come from the same source configs the ingestors use
(single source of truth). ``pyspark`` is imported inside the functions so this file
imports without Spark.
"""
from __future__ import annotations

from payments_platform.bronze import source_config
from payments_platform.orchestration.gates import (
    DEFAULT_BRONZE_POLICY, DEFAULT_SILVER_POLICY)
from payments_platform.databricks import (
    bronze_jdbc, bronze_rest_api, bronze_salesforce, bronze_sftp)

# audit columns every Bronze table must carry
REQUIRED_AUDIT = ["source_system", "run_id", "ingestion_timestamp", "record_hash"]

BRONZE_POLICY = {**DEFAULT_BRONZE_POLICY,
                 "required_sources": ["payments_file", "customer_cdc"],   # always seeded
                 "max_corrupt_files": 0}
SILVER_POLICY = {**DEFAULT_SILVER_POLICY}

WATERMARK_TABLE = bronze_jdbc.WATERMARK_TABLE
PROCESSED_FILES_TABLE = bronze_sftp.PROCESSED_FILES_TABLE


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _exists(spark, fqn):
    return spark.catalog.tableExists(fqn)


def _scalar(spark, sql):
    return spark.sql(sql).collect()[0][0]


def _result(checks, records_checked, quarantine_rate_pct, kind, load_status=None):
    failed = ["%s: %s" % (c["name"], c["detail"]) for c in checks if not c["passed"]]
    passed = len(failed) == 0
    return {
        "gate": kind,
        "gate_status": "PASS" if passed else "FAIL",
        "gate_passed": "true" if passed else "false",
        "records_checked": int(records_checked),
        "failed_checks": failed,
        "quarantine_rate_pct": round(float(quarantine_rate_pct), 2),
        "message": "%s gate %s — %d rows checked, %d/%d checks failed%s"
                   % (kind, "PASS" if passed else "FAIL", records_checked,
                      len(failed), len(checks),
                      ("" if passed else ": " + "; ".join(failed))),
        "checks": checks,
        "load_status": load_status or [],
    }


def _bronze_targets(catalog):
    """(watermark_key, bronze_table, watermark_col) for every enabled source —
    built from the same configs the ingestors read."""
    t = [("payments_file", "bronze.payments_file", None),
         ("customer_cdc", "bronze.customer_cdc", None)]
    for c in source_config.enabled_configs(
            source_config.load_source_config(bronze_jdbc._DEFAULT_CONFIG)):
        t.append((c["source_table"], c["target_bronze_table"], c.get("watermark_column")))
    for c in source_config.enabled_configs(source_config.load_configs(
            bronze_rest_api._DEFAULT_CONFIG, source_config.REST_REQUIRED_KEYS)):
        t.append(("%s.%s" % (c["api_name"], c["endpoint"]),
                  c["target_bronze_table"], (c.get("watermark") or {}).get("field")))
    for c in source_config.enabled_configs(source_config.load_configs(
            bronze_sftp._DEFAULT_CONFIG, source_config.SFTP_REQUIRED_KEYS)):
        t.append((c["source_system"], c["target_bronze_table"], None))
    for c in source_config.enabled_configs(source_config.load_configs(
            bronze_salesforce._DEFAULT_CONFIG, source_config.SALESFORCE_REQUIRED_KEYS)):
        t.append((c["source_object"], c["target_bronze_table"], c.get("watermark_column")))
    return t


# --------------------------------------------------------------------------- #
# Bronze validation gate
# --------------------------------------------------------------------------- #
def bronze_gate(spark, catalog, run_id, policy=None):
    p = {**BRONZE_POLICY, **(policy or {})}
    checks, total_rows = [], 0
    counts, load_status = {}, []                    # source_key -> row count

    for key, table, wm_col in _bronze_targets(catalog):
        fqn = "%s.%s" % (catalog, table)
        if not _exists(spark, fqn):
            counts[key] = None
            load_status.append({"table": table, "source": key, "records": 0, "status": "MISSING"})
            continue
        n = _scalar(spark, "SELECT count(*) FROM %s" % fqn)
        counts[key] = n
        total_rows += n
        load_status.append({"table": table, "source": key, "records": n,
                            "status": "LOADED" if n else "EMPTY"})
        # required audit columns present
        cols = set(spark.table(fqn).columns)
        missing = [a for a in REQUIRED_AUDIT if a not in cols]
        checks.append({"name": "audit_columns[%s]" % table, "passed": not missing,
                       "detail": "missing %s" % missing if missing else "ok"})
        # watermark not ahead of the extracted max
        if wm_col and wm_col in cols and _exists(spark, "%s.%s" % (catalog, WATERMARK_TABLE)):
            wm = spark.sql("SELECT watermark_value FROM %s.%s WHERE source_table = '%s'"
                           % (catalog, WATERMARK_TABLE, key)).collect()
            if wm and wm[0][0] is not None:
                bmax = _scalar(spark, "SELECT CAST(max(%s) AS string) FROM %s" % (wm_col, fqn))
                ahead = bmax is not None and wm[0][0] > bmax
                checks.append({"name": "watermark_not_ahead[%s]" % key, "passed": not ahead,
                               "detail": "wm=%s max=%s" % (wm[0][0], bmax)})

    # required sources produced rows
    for src in p["required_sources"]:
        n = counts.get(src)
        checks.append({"name": "required_source[%s]" % src, "passed": bool(n),
                       "detail": "rows=%s" % n})

    # total row sanity
    checks.append({"name": "min_total_rows", "passed": total_rows >= p["min_total_rows"],
                   "detail": "total=%d min=%d" % (total_rows, p["min_total_rows"])})

    # no unexpected corrupt-file failures
    pf = "%s.%s" % (catalog, PROCESSED_FILES_TABLE)
    if _exists(spark, pf):
        corrupt = _scalar(spark, "SELECT count(*) FROM %s WHERE status = 'CORRUPT'" % pf)
        checks.append({"name": "corrupt_files", "passed": corrupt <= p["max_corrupt_files"],
                       "detail": "corrupt=%d max=%d" % (corrupt, p["max_corrupt_files"])})

    return _result(checks, total_rows, 0.0, "bronze", load_status)


# --------------------------------------------------------------------------- #
# Silver DQ gate
# --------------------------------------------------------------------------- #
def silver_dq_gate(spark, catalog, run_id, policy=None):
    p = {**SILVER_POLICY, **(policy or {})}
    checks, records_checked, quar_rate, load_status = [], 0, 0.0, []

    clean = "%s.silver_clean.payment_clean" % catalog
    quar = "%s.silver_quarantine.failed_records" % catalog
    scd2 = "%s.silver_cdc.customer_scd2" % catalog

    # ---- payments DQ / quarantine ----
    if _exists(spark, clean):
        clean_n = _scalar(spark, "SELECT count(*) FROM %s" % clean)
        quar_n = _scalar(spark, "SELECT count(*) FROM %s" % quar) if _exists(spark, quar) else 0
        records_checked += clean_n + quar_n
        load_status.append({"table": "silver_clean.payment_clean", "source": "payments",
                            "records": clean_n, "status": "LOADED" if clean_n else "EMPTY"})
        load_status.append({"table": "silver_quarantine.failed_records", "source": "quarantine",
                            "records": quar_n, "status": "LOADED" if quar_n else "EMPTY"})
        denom = clean_n + quar_n
        quar_rate = (quar_n / denom * 100.0) if denom else 0.0
        checks.append({"name": "quarantine_rate",
                       "passed": quar_rate <= p["max_quarantine_rate_pct"],
                       "detail": "%.1f%% (max %.1f%%) clean=%d quar=%d"
                                 % (quar_rate, p["max_quarantine_rate_pct"], clean_n, quar_n)})
        dup = _scalar(spark, "SELECT count(*) FROM (SELECT payment_id FROM %s "
                             "GROUP BY payment_id HAVING count(*) > 1)" % clean)
        checks.append({"name": "duplicate_payment_ids", "passed": dup == 0, "detail": "dups=%d" % dup})
        nulls = _scalar(spark, "SELECT count(*) FROM %s WHERE payment_id IS NULL "
                               "OR customer_id IS NULL OR amount IS NULL" % clean)
        checks.append({"name": "null_critical_payment_cols", "passed": nulls == 0, "detail": "nulls=%d" % nulls})

    # ---- customer SCD2 validity ----
    if _exists(spark, scd2):
        scd2_n = _scalar(spark, "SELECT count(*) FROM %s" % scd2)
        records_checked += scd2_n
        load_status.append({"table": "silver_cdc.customer_scd2", "source": "customer",
                            "records": scd2_n, "status": "LOADED" if scd2_n else "EMPTY"})
        dup_cur = _scalar(spark, "SELECT count(*) FROM (SELECT customer_id FROM %s "
                                 "WHERE is_current GROUP BY customer_id HAVING count(*) > 1)" % scd2)
        checks.append({"name": "one_current_per_customer", "passed": dup_cur == 0, "detail": "multi_current=%d" % dup_cur})
        null_key = _scalar(spark, "SELECT count(*) FROM %s WHERE customer_id IS NULL" % scd2)
        checks.append({"name": "null_customer_id", "passed": null_key == 0, "detail": "nulls=%d" % null_key})
        bad_dates = _scalar(spark, "SELECT count(*) FROM %s WHERE effective_to IS NOT NULL "
                                   "AND effective_to < effective_from" % scd2)
        checks.append({"name": "effective_from_le_to", "passed": bad_dates == 0, "detail": "bad=%d" % bad_dates})
        open_cur = _scalar(spark, "SELECT count(*) FROM %s WHERE is_current = true "
                                  "AND effective_to IS NOT NULL" % scd2)
        checks.append({"name": "current_row_open", "passed": open_cur == 0, "detail": "closed_current=%d" % open_cur})
        back = _scalar(spark, "SELECT count(*) FROM (SELECT sequence_number, LAG(sequence_number) "
                              "OVER (PARTITION BY customer_id ORDER BY effective_from) AS prev "
                              "FROM %s) WHERE prev IS NOT NULL AND sequence_number < prev" % scd2)
        checks.append({"name": "sequence_forward_moving", "passed": back == 0, "detail": "out_of_order=%d" % back})
        deleted = _scalar(spark, "SELECT count(*) FROM %s WHERE is_current AND is_deleted" % scd2)
        checks.append({"name": "deleted_rows_current_consistent", "passed": True,
                       "detail": "current_soft_deleted=%d (informational)" % deleted})

    return _result(checks, records_checked, quar_rate, "silver_dq", load_status)


# --------------------------------------------------------------------------- #
# persist gate output -> Delta control tables (audit / history)
# --------------------------------------------------------------------------- #
def persist_gate_result(spark, catalog, run_id, task_name, result):
    """Append the gate outcome to the Delta control tables (best-effort audit;
    the orchestration decision is the task value, not this write):

      * ``silver_control.pipeline_run_audit`` — one summary row per gate run,
      * ``silver_control.dq_results``          — one row per individual check,
      * ``silver_control.table_load_status``   — one row per table the gate observed.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                                    StructField, StructType)

    gate = result.get("gate", task_name)
    ts = F.current_timestamp()

    def _append(fqn, rows, schema):
        if not rows:
            return
        (spark.createDataFrame(rows, schema).withColumn("check_timestamp", ts)
            .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fqn))

    _append("%s.silver_control.pipeline_run_audit" % catalog,
            [(run_id, task_name, gate, result["gate_status"], result["gate_passed"] == "true",
              int(result["records_checked"]), "; ".join(result["failed_checks"]),
              float(result["quarantine_rate_pct"]), result["message"])],
            StructType([
                StructField("run_id", StringType()), StructField("task_name", StringType()),
                StructField("gate_name", StringType()), StructField("gate_status", StringType()),
                StructField("gate_passed", BooleanType()), StructField("records_checked", LongType()),
                StructField("failed_checks", StringType()), StructField("quarantine_rate_pct", DoubleType()),
                StructField("message", StringType())]))

    _append("%s.silver_control.dq_results" % catalog,
            [(run_id, task_name, gate, c["name"], bool(c["passed"]), c["detail"])
             for c in result.get("checks", [])],
            StructType([
                StructField("run_id", StringType()), StructField("task_name", StringType()),
                StructField("gate_name", StringType()), StructField("check_name", StringType()),
                StructField("passed", BooleanType()), StructField("detail", StringType())]))

    _append("%s.silver_control.table_load_status" % catalog,
            [(run_id, task_name, ls["table"], ls.get("source"), int(ls["records"]), ls["status"])
             for ls in result.get("load_status", [])],
            StructType([
                StructField("run_id", StringType()), StructField("task_name", StringType()),
                StructField("table_name", StringType()), StructField("source_key", StringType()),
                StructField("records", LongType()), StructField("status", StringType())]))

    print("gate audit: persisted %s for run %s (audit + %d checks + %d tables)"
          % (task_name, run_id, len(result.get("checks", [])), len(result.get("load_status", []))))


_AUDIT_SCHEMA_COLS = ["run_id", "task_name", "gate_name", "gate_status", "gate_passed",
                      "records_checked", "failed_checks", "quarantine_rate_pct", "message"]


def record_pipeline_run(spark, catalog, run_id, task_name, gate_status, message, gate_passed=None):
    """Append a pipeline-level row to ``silver_control.pipeline_run_audit`` so the
    audit spans the whole job (STARTED at init_run, SUCCESS/PARTIAL at write_status),
    not just the gates. Same schema as the gate rows; ``gate_name = 'pipeline_run'``."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                                    StructField, StructType)
    schema = StructType([
        StructField("run_id", StringType()), StructField("task_name", StringType()),
        StructField("gate_name", StringType()), StructField("gate_status", StringType()),
        StructField("gate_passed", BooleanType()), StructField("records_checked", LongType()),
        StructField("failed_checks", StringType()), StructField("quarantine_rate_pct", DoubleType()),
        StructField("message", StringType())])
    row = [(run_id, task_name, "pipeline_run", gate_status, gate_passed, 0, "", 0.0, message)]
    (spark.createDataFrame(row, schema).withColumn("check_timestamp", F.current_timestamp())
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable("%s.silver_control.pipeline_run_audit" % catalog))


def pipeline_run_status(spark, catalog, run_id):
    """Derive the finish status from the persisted gate rows: PARTIAL if any gate
    blocked (gate_passed = false), else SUCCESS."""
    audit = "%s.silver_control.pipeline_run_audit" % catalog
    if not spark.catalog.tableExists(audit):
        return "SUCCESS"
    fails = _scalar(spark, "SELECT count(*) FROM %s WHERE run_id = '%s' "
                           "AND gate_name IN ('bronze', 'silver_dq') AND gate_passed = false"
                           % (audit, run_id))
    return "PARTIAL" if fails else "SUCCESS"
