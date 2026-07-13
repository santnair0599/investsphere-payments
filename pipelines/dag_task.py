"""
Single parameterized task entry-point for the Lakeflow Job.

    spark_python_task: { python_file: ./pipelines/dag_task.py,
                         parameters: ["--task", "bronze_jdbc", ...] }

Each Databricks task runs this with a different ``--task``. In production each
task reads its upstream Delta tables and writes its downstream tables + control
rows; gates publish a task VALUE that a condition task branches on. Here the
bronze/silver/gate bodies log their intent, while **governance_validation runs
for real and fails the task (exit 1) on any PII/access violation** — so the gate
is enforced in CI/deploy, not just documented.

The task graph mirrors ``payments_platform.orchestration.dag`` 1:1
(see docs/ORCHESTRATION.md).
"""
from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))

from job_params import get_param                                  # noqa: E402
from payments_platform.governance import validate as gov_validate  # noqa: E402


def _set_task_value(key, value):
    """Publish a gate outcome as a Databricks task value (no-op locally)."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        dbutils.jobs.taskValues.set(key=key, value=value)
    except Exception:
        print("[task-value] %s = %s (local; would set in Databricks)" % (key, value))


def _publish_gate(result):
    """Publish the gate result as the full set of task values a condition task
    (and monitoring) can read."""
    for key in ("gate_status", "gate_passed", "records_checked", "failed_checks",
                "quarantine_rate_pct", "message"):
        _set_task_value(key, result[key])


def _persist_gate(spark, params, task_name, result):
    """Best-effort: write the gate result to the Delta control tables. A failure
    here must NOT change the gate's orchestration outcome (the task value)."""
    from payments_platform.databricks.gates import persist_gate_result
    try:
        persist_gate_result(spark, params["catalog"], params["run_id"], task_name, result)
    except Exception as exc:  # noqa: BLE001 — audit write is non-blocking
        print("%s: gate-audit persist skipped: %s" % (task_name, exc))


def task_init_run(params):
    print("init_run:", params["run_id"], "env=", params["env"],
          "load_mode=", params["load_mode"])
    try:
        from pyspark.sql import SparkSession
        from payments_platform.databricks.gates import record_pipeline_run
        record_pipeline_run(
            SparkSession.builder.getOrCreate(), params["catalog"], params["run_id"],
            "init_run", "STARTED",
            "pipeline started env=%s load_mode=%s run_date=%s"
            % (params["env"], params["load_mode"], params["run_date"]))
    except Exception as exc:  # noqa: BLE001 — audit write is non-blocking
        print("init_run: pipeline-run audit skipped:", exc)


def task_bronze(params, source):
    # All six source patterns are IMPLEMENTED with real Spark.
    # Two Auto Loader lanes: campaigns feed the enterprise entertainment domain;
    # payments feeds the retained payments reference star (see docs/ENTERPRISE_PIVOT.md).
    if source == "bronze_campaign_file":
        from payments_platform.databricks.bronze_campaign_autoloader import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    if source == "bronze_payments_file":
        from payments_platform.databricks.bronze_payments_autoloader import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    if source == "bronze_customer_cdc":
        from payments_platform.databricks.bronze_customer_cdc import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    if source == "bronze_jdbc":
        from payments_platform.databricks.bronze_jdbc import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    if source == "bronze_rest_api":
        from payments_platform.databricks.bronze_rest_api import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    if source == "bronze_sftp":
        from payments_platform.databricks.bronze_sftp import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    if source == "bronze_salesforce":
        from payments_platform.databricks.bronze_salesforce import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    # every configured Bronze source is now implemented; this guards a typo'd task
    print("bronze:%s -> NOT IMPLEMENTED YET (reference stub; load_mode=%s, backfill=%s..%s)"
          % (source, params["load_mode"],
             params["backfill_start_date"], params["backfill_end_date"]))


def task_bronze_validation_gate(params):
    # Real gate: row counts, audit columns, corrupt-file status, watermark sanity.
    from pyspark.sql import SparkSession
    from payments_platform.databricks.gates import bronze_gate, persist_gate_result
    spark = SparkSession.builder.getOrCreate()
    result = bronze_gate(spark, params["catalog"], params["run_id"])
    _publish_gate(result)                                   # orchestration signal (unchanged)
    _persist_gate(spark, params, "bronze_validation_gate", result)
    print("bronze_validation_gate:", result["message"])


def task_silver(params, entity):
    # Enterprise Silver conformers — each mirrors the payments pattern
    # (parse/DQ/quarantine/dedup/MERGE). customer SCD2 is the shared guest/customer
    # dimension (Delta SCD2 MERGE from Debezium CDC).
    if entity == "silver_customer_scd2":
        from payments_platform.databricks.silver_customer_scd2 import run
        run(catalog=params["catalog"], run_id=params["run_id"])
        return
    # domain conformers (real estate / hospitality / entertainment / investment / customer CRM)
    _DOMAIN_CONFORMERS = {
        "silver_realestate":    "payments_platform.databricks.silver_realestate",
        "silver_hospitality":   "payments_platform.databricks.silver_hospitality",
        "silver_entertainment": "payments_platform.databricks.silver_entertainment",
        "silver_investment":    "payments_platform.databricks.silver_investment",
        "silver_customer":      "payments_platform.databricks.silver_customer",
        "silver_payments":      "payments_platform.databricks.silver_payments",  # legacy, retained
    }
    module = _DOMAIN_CONFORMERS.get(entity)
    if module:
        import importlib
        importlib.import_module(module).run(catalog=params["catalog"], run_id=params["run_id"])
        return
    print("silver:%s -> NOT IMPLEMENTED YET (reference stub)" % entity)


def task_silver_dq_gate(params):
    # Real gate: quarantine rate, duplicate/null keys, SCD2 validity.
    from pyspark.sql import SparkSession
    from payments_platform.databricks.gates import silver_dq_gate, persist_gate_result
    spark = SparkSession.builder.getOrCreate()
    result = silver_dq_gate(spark, params["catalog"], params["run_id"])
    _publish_gate(result)                                   # orchestration signal (unchanged)
    _persist_gate(spark, params, "silver_dq_gate", result)
    print("silver_dq_gate:", result["message"])


def task_governance_validation(params):
    violations = gov_validate.validate_all()
    if violations:
        print("GOVERNANCE FAILED — PII/access violations:")
        for v in violations:
            print("  -", v)
        sys.exit(1)
    print("governance_validation: 0 violations")


def task_publish_notify(params):
    print("publish_notify: refresh BI / send success notification")


def task_write_status(params):
    # Close out the run: derive the finish status from the persisted gate rows and
    # append the pipeline-run FINISH row (best-effort audit; runs ALL_DONE).
    try:
        from pyspark.sql import SparkSession
        from payments_platform.databricks.gates import pipeline_run_status, record_pipeline_run
        spark = SparkSession.builder.getOrCreate()
        status = pipeline_run_status(spark, params["catalog"], params["run_id"])
        record_pipeline_run(spark, params["catalog"], params["run_id"], "write_status",
                            status, "pipeline finished: %s" % status,
                            gate_passed=(status == "SUCCESS"))
        print("write_status: pipeline_run_audit FINISH row (%s) for %s" % (status, params["run_id"]))
    except Exception as exc:  # noqa: BLE001 — audit write is non-blocking
        print("write_status: pipeline-run audit skipped:", exc)


def main():
    params = {
        "task": get_param("task", "init_run"),
        "env": get_param("env", "dev"),
        "catalog": get_param("catalog", "investsphere"),
        "run_date": get_param("run_date", "2026-06-30"),
        "run_id": get_param("run_id", "local-run"),
        "load_mode": get_param("load_mode", "incremental"),
        "backfill_start_date": get_param("backfill_start_date", ""),
        "backfill_end_date": get_param("backfill_end_date", ""),
    }
    task = params["task"]
    if task == "init_run":
        task_init_run(params)
    elif task in ("bronze_campaign_file", "bronze_payments_file", "bronze_jdbc",
                  "bronze_customer_cdc", "bronze_rest_api", "bronze_sftp",
                  "bronze_salesforce"):
        task_bronze(params, task)
    elif task == "bronze_validation_gate":
        task_bronze_validation_gate(params)
    elif task in ("silver_payments", "silver_customer_scd2", "silver_realestate",
                  "silver_hospitality", "silver_entertainment", "silver_investment",
                  "silver_customer"):
        task_silver(params, task)
    elif task == "silver_dq_gate":
        task_silver_dq_gate(params)
    elif task == "governance_validation":
        task_governance_validation(params)
    elif task == "publish_notify":
        task_publish_notify(params)
    elif task == "write_status":
        task_write_status(params)
    else:
        raise SystemExit("unknown task: " + task)


if __name__ == "__main__":
    main()
