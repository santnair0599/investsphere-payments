"""
Task handlers for the DAG — thin wrappers that call the platform library and
stash domain results in a shared ``state`` dict so gates can read them.

Each handler is ``fn(params, results) -> TaskResult``. The bundle's per-task
Python scripts call the same underlying functions; here they're composed for the
local end-to-end run and the tests.
"""
from __future__ import annotations

import json
import os

from payments_platform.bronze import (
    file_ingest, cdc_ingest, jdbc_ingest, rest_ingest, sftp_ingest,
    salesforce_ingest)
from payments_platform.silver import parse, dedup, quarantine
from payments_platform.silver.dq import PAYMENT_RULES
from payments_platform.silver.cdc_apply import apply_scd2
from payments_platform.governance import validate as gov_validate
from payments_platform.orchestration import gates
from payments_platform.orchestration.runner import TaskResult, SUCCESS, FAILED

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))
PAYMENTS_FILE = os.path.join(_ROOT, "seeds", "payments", "payments_2026-06-30.csv")
CDC_FILE = os.path.join(_ROOT, "seeds", "cdc", "customer_cdc_events.json")

PAYMENT_COLS = ["payment_id", "customer_id", "account_id", "amount",
                "currency", "payment_type", "transaction_date"]
CUSTOMER_COLS = ["id", "name", "email", "phone", "country", "status"]
CUSTOMER_TRACKED = ["customer_name", "email", "phone_number", "nationality", "status"]


def backfill_from_params(params):
    """Build the backfill dict iff load_mode == 'backfill' (else None)."""
    if params.get("load_mode") == "backfill":
        return {"start_date": params["backfill_start_date"],
                "end_date": params["backfill_end_date"]}
    return None


def bronze_jdbc_load(params, source, watermark_store, ctx):
    """Run the metadata-driven JDBC For-Each ingestor, passing backfill through.

    This is the unit-tested seam proving backfill params reach the ingestor.
    """
    configs = params["jdbc_configs"]
    return jdbc_ingest.run_ingestion(
        configs, source, watermark_store, ctx,
        backfill=backfill_from_params(params))


# --------------------------------------------------------------------------
# Handler factory for the local end-to-end run
# --------------------------------------------------------------------------
def build_handlers(ctx, jdbc_source=None, jdbc_configs=None, watermark_store=None,
                   rest_client=None, rest_configs=None,
                   sftp_client=None, sftp_configs=None, sftp_expected_date=None,
                   sfdc_client=None, sfdc_configs=None,
                   processed_files=None):
    state = {"bronze": {}, "silver": {}}
    watermark_store = watermark_store or jdbc_ingest.WatermarkStore()
    processed_files = processed_files or sftp_ingest.ProcessedFileRegistry()

    def init_run(params, results):
        params.setdefault("state", state)
        return TaskResult(SUCCESS, info="run " + ctx.run_id)

    def bronze_payments_file(params, results):
        bronze = file_ingest.ingest_file(PAYMENTS_FILE, ctx, PAYMENT_COLS)
        corrupt = [r for r in bronze if r["_corrupt_record"]]
        state["bronze"]["payments_file"] = bronze
        return TaskResult(SUCCESS, records_written=len(bronze),
                          info={"source": "payments_file", "status": SUCCESS,
                                "records_written": len(bronze),
                                "corrupt_records": len(corrupt)})

    def bronze_jdbc(params, results):
        if jdbc_source is None or jdbc_configs is None:
            return TaskResult(SUCCESS, records_written=0,
                              info={"source": "jdbc", "status": SUCCESS,
                                    "records_written": 0, "corrupt_records": 0})
        params = {**params, "jdbc_configs": jdbc_configs}
        res = bronze_jdbc_load(params, jdbc_source, watermark_store, ctx)
        written = sum(r.get("records_written", 0) for r in res)
        failed = any(r["status"] == "FAILED" for r in res)
        state["bronze"]["jdbc"] = res
        return TaskResult(FAILED if failed else SUCCESS, records_written=written,
                          info={"source": "jdbc",
                                "status": "FAILED" if failed else SUCCESS,
                                "records_written": written, "corrupt_records": 0})

    def bronze_customer_cdc(params, results):
        with open(CDC_FILE, "r", encoding="utf-8") as handle:
            events = json.load(handle)
        bronze = cdc_ingest.normalize_events(events, ctx, CUSTOMER_COLS,
                                             topic="cdc.public.customer")
        state["bronze"]["customer_cdc"] = bronze
        return TaskResult(SUCCESS, records_written=len(bronze),
                          info={"source": "customer_cdc", "status": SUCCESS,
                                "records_written": len(bronze), "corrupt_records": 0})

    def _noop_bronze(source):
        return TaskResult(SUCCESS, records_written=0,
                          info={"source": source, "status": SUCCESS,
                                "records_written": 0, "corrupt_records": 0})

    def bronze_rest_api(params, results):
        if rest_client is None or not rest_configs:
            return _noop_bronze("rest_api")
        res = rest_ingest.run_ingestion(rest_configs, rest_client,
                                        watermark_store, ctx)
        written = sum(r.get("records_written", 0) for r in res)
        failed = any(r["status"] == "FAILED" for r in res)
        state["bronze"]["rest_api"] = res
        return TaskResult(FAILED if failed else SUCCESS, records_written=written,
                          info={"source": "rest_api",
                                "status": "FAILED" if failed else SUCCESS,
                                "records_written": written, "corrupt_records": 0})

    def bronze_sftp(params, results):
        if sftp_client is None or not sftp_configs:
            return _noop_bronze("sftp")
        expected = sftp_expected_date or params.get("run_date")
        res = [sftp_ingest.ingest_directory(c, sftp_client, ctx, processed_files,
                                            expected) for c in sftp_configs]
        written = sum(r.get("records_written", 0) for r in res)
        corrupt = sum(r.get("corrupt_records", 0) for r in res)
        failed = any(r["status"] == "FAILED" for r in res)
        state["bronze"]["sftp"] = res
        return TaskResult(FAILED if failed else SUCCESS, records_written=written,
                          info={"source": "sftp",
                                "status": "FAILED" if failed else SUCCESS,
                                "records_written": written,
                                "corrupt_records": corrupt})

    def bronze_salesforce(params, results):
        if sfdc_client is None or not sfdc_configs:
            return _noop_bronze("salesforce")
        res = salesforce_ingest.run_ingestion(sfdc_configs, sfdc_client,
                                              watermark_store, ctx)
        written = sum(r.get("records_written", 0) for r in res)
        failed = any(r["status"] == "FAILED" for r in res)
        state["bronze"]["salesforce"] = res
        return TaskResult(FAILED if failed else SUCCESS, records_written=written,
                          info={"source": "salesforce",
                                "status": "FAILED" if failed else SUCCESS,
                                "records_written": written, "corrupt_records": 0})

    def bronze_validation_gate(params, results):
        bronze_results = [results[k].info for k in
                          ("bronze_payments_file", "bronze_jdbc", "bronze_customer_cdc")
                          if isinstance(results.get(k), TaskResult)
                          and isinstance(results[k].info, dict)]
        policy = {"required_sources": ["payments_file", "customer_cdc"],
                  "max_corrupt_records": 50, "min_total_rows": 1}
        passed, reasons = gates.bronze_validation_gate(bronze_results, policy)
        return TaskResult(SUCCESS, outcome=passed, info={"reasons": reasons})

    def silver_payments(params, results):
        bronze = state["bronze"].get("payments_file", [])
        clean = [r for r in bronze if not r["_corrupt_record"]]
        parsed = [parse.parse_payment(r) for r in clean]
        deduped = dedup.dedup_latest(parsed, ["payment_id"], "ingestion_timestamp")
        try:
            valid, quarantined = quarantine.split(
                deduped, PAYMENT_RULES, ctx, "bronze.payments")
            state["silver"]["payments"] = {
                "entity": "payments", "valid": len(valid),
                "quarantined": len(quarantined), "fail_severity": False}
            return TaskResult(SUCCESS, records_written=len(valid))
        except Exception as exc:  # FAIL-severity breach
            state["silver"]["payments"] = {
                "entity": "payments", "valid": 0, "quarantined": 0,
                "fail_severity": True}
            return TaskResult(FAILED, info=str(exc))

    def silver_customer_scd2(params, results):
        bronze = state["bronze"].get("customer_cdc", [])
        conformed = [parse.standardize_customer(r) for r in bronze]
        scd2 = apply_scd2([], conformed, "customer_id", CUSTOMER_TRACKED)
        current = [r for r in scd2 if r["is_current"]]
        state["silver"]["customer"] = {
            "entity": "customer", "valid": len(current), "quarantined": 0,
            "fail_severity": False}
        return TaskResult(SUCCESS, records_written=len(scd2))

    def silver_dq_gate(params, results):
        silver_results = list(state["silver"].values())
        passed, reasons = gates.silver_dq_gate(
            silver_results, params.get("silver_policy"))
        return TaskResult(SUCCESS, outcome=passed, info={"reasons": reasons})

    def dbt_build(params, results):
        # dbt runs against a SQL warehouse in prod; locally a no-op success.
        return TaskResult(SUCCESS, info="dbt build gold+ (warehouse in prod)")

    def dbt_test(params, results):
        return TaskResult(SUCCESS, info="dbt test (fails job on test failure in prod)")

    def governance_validation(params, results):
        violations = gov_validate.validate_all()
        if violations:
            return TaskResult(FAILED, info={"violations": violations})
        return TaskResult(SUCCESS, info="0 PII/access violations")

    def publish_notify(params, results):
        return TaskResult(SUCCESS, info="published + notified")

    return {
        "init_run": init_run,
        "bronze_payments_file": bronze_payments_file,
        "bronze_jdbc": bronze_jdbc,
        "bronze_customer_cdc": bronze_customer_cdc,
        "bronze_rest_api": bronze_rest_api,
        "bronze_sftp": bronze_sftp,
        "bronze_salesforce": bronze_salesforce,
        "bronze_validation_gate": bronze_validation_gate,
        "silver_payments": silver_payments,
        "silver_customer_scd2": silver_customer_scd2,
        "silver_dq_gate": silver_dq_gate,
        "dbt_build": dbt_build,
        "dbt_test": dbt_test,
        "governance_validation": governance_validation,
        "publish_notify": publish_notify,
    }
