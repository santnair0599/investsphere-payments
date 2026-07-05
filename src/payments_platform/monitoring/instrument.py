"""
Wire the :class:`RunMonitor` into the orchestration DAG.

``run_monitored_dag`` wraps each task handler so that, as the DAG executes:

  * ``init_run``               -> pipeline_run STARTED
  * every task                 -> task_run (status, records, retries, errors)
  * Bronze tasks               -> table_load_status (row counts + corrupt/rejected)
  * Bronze validation gate     -> dq_results (stage=bronze_gate, PASS/FAIL + reasons)
  * Silver tasks               -> table_load_status + quarantine_summary
  * Silver DQ gate             -> dq_results per entity (stage=silver, failed rule+count)
  * dbt build / dbt test       -> dbt_results
  * governance_validation      -> security_events (violations -> VIOLATION)
  * publish / final            -> pipeline_run final SUCCESS/FAILED/PARTIAL

The wrappers read the shared ``state`` dict (``params['state']`` populated by
``tasks.build_handlers``) for quarantine/DQ counts, and degrade gracefully when a
handler returns a bare TaskResult (so the fake-handler orchestration tests still
work). Nothing in ``orchestration/`` is modified — this layer is purely additive.
"""
from __future__ import annotations

from payments_platform.orchestration import dag as D
from payments_platform.orchestration.runner import (
    run_dag, TaskResult, SUCCESS, FAILED, SKIPPED)
from payments_platform.monitoring.recorder import RunMonitor
from payments_platform.monitoring.models import PARTIAL

BRONZE_TASKS = {"bronze_payments_file", "bronze_jdbc", "bronze_customer_cdc",
                "bronze_rest_api", "bronze_sftp", "bronze_salesforce"}
SILVER_TASKS = {"silver_payments", "silver_customer_scd2"}


def derive_final_status(results):
    """SUCCESS if everything ran; FAILED on any hard task failure; PARTIAL when a
    gate blocked part of the flow (downstream SKIPPED) with no hard failure."""
    statuses = [r.status for r in results.values()]
    if any(s == FAILED for s in statuses):
        return FAILED
    if any(s == SKIPPED for s in statuses):
        return PARTIAL
    return SUCCESS


def _phase(key):
    try:
        return D.get_task(key)["phase"]
    except StopIteration:
        return None


def _err(res):
    if res.status == FAILED:
        return res.info if isinstance(res.info, str) else str(res.info)
    return None


def _emit_domain(key, res, monitor, ctx, params, spike_threshold_pct):
    """Write the model-specific record(s) for a task that just executed."""
    state = params.get("state", {}) if isinstance(params, dict) else {}

    # ---- Bronze: table_load_status (row counts + corrupt/rejected) ----
    if key in BRONZE_TASKS and isinstance(res.info, dict) and "source" in res.info:
        info = res.info
        corrupt = info.get("corrupt_records", 0)
        monitor.record_load_status(
            source_table=info["source"],
            target_table="bronze." + info["source"],
            load_type="batch",
            records_read=info.get("records_written", 0) + corrupt,
            records_written=info.get("records_written", 0),
            records_rejected=corrupt,
            corrupt_record_count=corrupt,
            status=info.get("status", res.status))
        return

    # ---- Bronze gate: dq_results (gate decision) ----
    if key == "bronze_validation_gate":
        reasons = (res.info or {}).get("reasons", []) if isinstance(res.info, dict) else []
        monitor.record_dq(
            entity="bronze", rule_name="bronze_validation_gate", severity="HIGH",
            records_evaluated=1, records_failed=0 if res.outcome else max(1, len(reasons)),
            stage="bronze_gate", action="BLOCK", detail=reasons)
        return

    # ---- Silver tasks: load status + quarantine summary ----
    if key in SILVER_TASKS:
        entity_state = _silver_entity(state, key)
        if entity_state:
            total = entity_state["valid"] + entity_state["quarantined"]
            monitor.record_load_status(
                source_table="bronze." + entity_state["entity"],
                target_table="silver." + entity_state["entity"],
                load_type="batch",
                records_read=total, records_written=entity_state["valid"],
                records_rejected=entity_state["quarantined"], status=res.status)
            monitor.record_quarantine(
                entity=entity_state["entity"],
                source_table="silver." + entity_state["entity"],
                records_total=total, records_quarantined=entity_state["quarantined"],
                spike_threshold_pct=spike_threshold_pct)
        return

    # ---- Silver DQ gate: per-entity dq_results (failed rule + failed count) ----
    if key == "silver_dq_gate":
        reasons = (res.info or {}).get("reasons", []) if isinstance(res.info, dict) else []
        gate_passed = res.outcome is not False
        from payments_platform.monitoring.models import PASS, FAIL
        for ent in state.get("silver", {}).values():
            entity = ent["entity"]
            total = ent["valid"] + ent["quarantined"]
            failed = ent["quarantined"]
            # this entity breached the gate if it raised a FAIL-severity rule or is
            # named in a gate reason (e.g. quarantine rate over threshold)
            breached = (not gate_passed) and (
                ent.get("fail_severity") or any(entity in r for r in reasons))
            if ent.get("fail_severity"):
                monitor.record_dq(entity, "fail_severity_rule", "FATAL",
                                  records_evaluated=max(1, total),
                                  records_failed=max(1, total), stage="silver",
                                  action="FAIL", result=FAIL if breached else PASS,
                                  detail=reasons)
            else:
                monitor.record_dq(entity, "quarantine_threshold", "HIGH",
                                  records_evaluated=total, records_failed=failed,
                                  stage="silver", action="QUARANTINE",
                                  result=FAIL if breached else PASS, detail=reasons)
        return

    # ---- dbt: dbt_results ----
    if key in ("dbt_build", "dbt_test"):
        command = "test" if key == "dbt_test" else "build"
        tests_failed = 0 if res.status == SUCCESS else _info_int(res, "tests_failed", 1)
        monitor.record_dbt(command, res.status,
                           tests_run=_info_int(res, "tests_run", 0),
                           tests_failed=tests_failed,
                           detail=res.info if isinstance(res.info, str) else None)
        return

    # ---- governance: security_events ----
    if key == "governance_validation":
        violations = (res.info or {}).get("violations", []) if isinstance(res.info, dict) else []
        monitor.record_security("governance_validation", violations, severity="CRITICAL")
        return


def _silver_entity(state, key):
    """Map a silver task key to its entry in state['silver']."""
    silver = state.get("silver", {})
    if key == "silver_payments":
        return silver.get("payments")
    if key == "silver_customer_scd2":
        return silver.get("customer")
    return None


def _info_int(res, field, default):
    if isinstance(res.info, dict):
        return res.info.get(field, default)
    return default


def _instrument(key, base_handler, monitor, ctx, spike_threshold_pct):
    phase = _phase(key)

    def handler(params, results):
        if key == "init_run" and not monitor.tables["pipeline_run"]:
            monitor.start_pipeline()
        try:
            res = base_handler(params, results)
        except Exception as exc:  # noqa: BLE001 — record then re-raise for the runner
            monitor.record_task(key, FAILED, phase=phase, error_message=str(exc))
            raise
        if not isinstance(res, TaskResult):
            res = TaskResult(info=res)
        monitor.record_task(key, res.status, phase=phase,
                           records_written=res.records_written, error_message=_err(res))
        _emit_domain(key, res, monitor, ctx, params, spike_threshold_pct)
        return res

    return handler


def run_monitored_dag(handlers, params, ctx, job_name,
                      monitor=None, tasks=None, spike_threshold_pct=50.0):
    """Run the DAG with full monitoring instrumentation.

    Returns ``(results, monitor)``. The monitor holds every model's rows and a
    finalised ``pipeline_run`` (SUCCESS/FAILED/PARTIAL).
    """
    params = params or {}
    monitor = monitor or RunMonitor(ctx, job_name)
    if not monitor.tables["pipeline_run"]:
        monitor.start_pipeline()

    wrapped = {k: _instrument(k, h, monitor, ctx, spike_threshold_pct)
               for k, h in handlers.items()}
    results = run_dag(wrapped, params, tasks)

    # backfill task_run for any enabled task the wrappers didn't record — SKIPPED
    # tasks (handler never executed) and tasks that ran via the runner's default
    # no-op handler (e.g. newly-enabled sources with no handler wired in this run).
    for key in D.topo_order(D.enabled_tasks(tasks)):
        res = results.get(key)
        if res is not None and monitor.task_attempts(key) == 0:
            monitor.record_task(key, res.status, phase=_phase(key),
                               records_written=res.records_written,
                               error_message=res.info if res.status != SUCCESS else None)

    final = derive_final_status(results)
    reason = None if final == SUCCESS else "see task_run / dq_results for blocking step"
    monitor.finalize_pipeline(final, reason=reason)
    return results, monitor
