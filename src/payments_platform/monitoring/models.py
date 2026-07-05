"""
The nine monitoring / control models — pure dict builders, trivially unit-tested.

Run-control models reuse :mod:`config.control_tables` (so there is one source of
truth shared with the ingestion control path):
    pipeline_run, task_run, table_load_status            -> silver_control.*

Observability models are defined here:
    dq_results, table_freshness, quarantine_summary,
    dbt_results, security_events, cost_summary           -> monitoring.*

``MODEL_TABLES`` maps each model to its Unity Catalog ``schema.table`` (under the
per-env catalog) so the recorder, dashboards and docs stay consistent.
"""
from __future__ import annotations

from payments_platform.config.control_tables import (  # noqa: F401  (re-export)
    pipeline_run, task_run, table_load_status,
    SUCCESS, FAILED, RUNNING, STARTED, PARTIAL, SKIPPED,
)

# DQ result outcomes
PASS = "PASS"
FAIL = "FAIL"
# security_events outcomes
CLEAN = "CLEAN"
VIOLATION = "VIOLATION"

# model name -> schema.table (relative to the per-environment catalog)
MODEL_TABLES = {
    "pipeline_run":       "silver_control.pipeline_run",
    "task_run":           "silver_control.task_run",
    "table_load_status":  "silver_control.table_load_status",
    "dq_results":         "monitoring.dq_results",
    "table_freshness":    "monitoring.table_freshness",
    "quarantine_summary": "monitoring.quarantine_summary",
    "dbt_results":        "monitoring.dbt_results",
    "security_events":    "monitoring.security_events",
    "cost_summary":       "monitoring.cost_summary",
}


def dq_results(ctx, entity, rule_name, severity, records_evaluated,
               records_failed, stage="silver", action="QUARANTINE",
               result=None, detail=None):
    """One row for ``monitoring.dq_results`` — a DQ rule's outcome on a table.

    ``records_failed`` is the count that violated ``rule_name``. ``result``
    (PASS/FAIL) defaults to "FAIL iff anything failed", but a gate can pass it
    explicitly so a normal, within-threshold quarantine still reads as PASS while
    a gate-breaching one reads as FAIL.
    """
    total = records_evaluated or 0
    failed = records_failed or 0
    if result is None:
        result = PASS if failed == 0 else FAIL
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.ingestion_timestamp[:10],
        "stage": stage,
        "entity": entity,
        "rule_name": rule_name,
        "severity": severity,
        "action": action,
        "records_evaluated": total,
        "records_failed": failed,
        "fail_rate_pct": round(failed / total * 100, 4) if total else 0.0,
        "result": result,
        "detail": detail,
        "event_time": ctx.ingestion_timestamp,
    }


def table_freshness(ctx, table, last_loaded_at, as_of, sla_minutes):
    """One row for ``monitoring.table_freshness`` — lag of a table vs its SLA.

    ``last_loaded_at`` and ``as_of`` are ISO timestamps passed in (no wall clock),
    so freshness is deterministic and testable.
    """
    lag = _minutes_between(last_loaded_at, as_of)
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.ingestion_timestamp[:10],
        "table_name": table,
        "last_loaded_at": last_loaded_at,
        "as_of": as_of,
        "lag_minutes": lag,
        "sla_minutes": sla_minutes,
        "sla_breached": lag is not None and lag > sla_minutes,
        "event_time": ctx.ingestion_timestamp,
    }


def quarantine_summary(ctx, entity, source_table, records_total,
                       records_quarantined, spike_threshold_pct=None):
    """One row for ``monitoring.quarantine_summary`` — quarantine share + spike."""
    total = records_total or 0
    quarantined = records_quarantined or 0
    rate = round(quarantined / total * 100, 4) if total else 0.0
    spike = spike_threshold_pct is not None and rate > spike_threshold_pct
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.ingestion_timestamp[:10],
        "entity": entity,
        "source_table": source_table,
        "records_total": total,
        "records_quarantined": quarantined,
        "quarantine_rate_pct": rate,
        "spike_threshold_pct": spike_threshold_pct,
        "is_spike": spike,
        "event_time": ctx.ingestion_timestamp,
    }


def dbt_results(ctx, command, status, models_run=0, tests_run=0,
                tests_passed=0, tests_failed=0, detail=None):
    """One row for ``monitoring.dbt_results`` — a dbt build/test invocation."""
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.ingestion_timestamp[:10],
        "command": command,                 # "build" | "test" | "run"
        "status": status,                   # SUCCESS | FAILED
        "models_run": models_run,
        "tests_run": tests_run,
        "tests_passed": tests_passed,
        "tests_failed": tests_failed,
        "detail": detail,
        "event_time": ctx.ingestion_timestamp,
    }


def security_events(ctx, check_name, violations, severity="HIGH", detail=None):
    """One row for ``monitoring.security_events`` — a governance/PII check result.

    ``violations`` is the list returned by ``governance.validate``; a non-empty
    list is a VIOLATION event.
    """
    violations = violations or []
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.ingestion_timestamp[:10],
        "check_name": check_name,
        "severity": severity,
        "status": CLEAN if not violations else VIOLATION,
        "violation_count": len(violations),
        "violations": list(violations),
        "detail": detail,
        "event_time": ctx.ingestion_timestamp,
    }


def cost_summary(ctx, warehouse, dbus, cost_usd, budget_usd=None,
                 threshold_pct=80.0):
    """One row for ``monitoring.cost_summary`` — spend vs budget for a run/day."""
    breached = (
        budget_usd is not None
        and cost_usd > budget_usd * threshold_pct / 100.0
    )
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.ingestion_timestamp[:10],
        "environment": ctx.environment,
        "warehouse": warehouse,
        "dbus": dbus,
        "cost_usd": round(float(cost_usd), 4),
        "budget_usd": budget_usd,
        "threshold_pct": threshold_pct,
        "budget_breached": breached,
        "event_time": ctx.ingestion_timestamp,
    }


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _minutes_between(start_iso, end_iso):
    """Whole minutes between two ISO timestamps (None if unparseable)."""
    import datetime as _dt

    try:
        start = _dt.datetime.fromisoformat(start_iso)
        end = _dt.datetime.fromisoformat(end_iso)
    except (TypeError, ValueError):
        return None
    return int((end - start).total_seconds() // 60)
