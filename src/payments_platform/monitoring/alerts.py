"""
Declarative alert rules + a pure evaluator.

``ALERT_RULES`` is policy-as-code: each rule has an id, severity, the routing
channels (email / teams / itsm), and a ``predicate(snapshot, thresholds)`` that
inspects a :meth:`RunMonitor.snapshot` and returns the list of triggering rows.
``evaluate_alerts`` runs every rule and returns the fired alerts.

Most thresholds (quarantine spike, freshness SLA, cost budget) are evaluated when
the row is recorded and stored as a boolean flag, so the predicates just read the
flag — the same number can't be interpreted two ways. ``thresholds`` lets a caller
override the spike percentage at evaluation time as well.
"""
from __future__ import annotations

from payments_platform.monitoring.models import FAIL, FAILED, VIOLATION

# severities
CRITICAL, HIGH, MEDIUM = "CRITICAL", "HIGH", "MEDIUM"

DEFAULT_THRESHOLDS = {
    "quarantine_spike_pct": 50.0,   # used only if a row has no precomputed flag
}


# --------------------------------------------------------------------------- #
# predicates: snapshot -> list of offending rows
# --------------------------------------------------------------------------- #
def _pipeline_failure(snap, _t):
    return [r for r in snap["pipeline_run"] if r["status"] == FAILED]


def _bronze_validation_failure(snap, _t):
    return [r for r in snap["dq_results"]
            if r["stage"] == "bronze_gate" and r["result"] == FAIL]


def _silver_dq_failure(snap, _t):
    return [r for r in snap["dq_results"]
            if r["stage"] in ("silver", "silver_gate") and r["result"] == FAIL]


def _dbt_test_failure(snap, _t):
    return [r for r in snap["dbt_results"]
            if r["command"] == "test" and (r["status"] == FAILED
                                           or r["tests_failed"] > 0)]


def _quarantine_spike(snap, thresholds):
    out = []
    for r in snap["quarantine_summary"]:
        if r["is_spike"]:
            out.append(r)
        elif r["spike_threshold_pct"] is None:
            # no per-row threshold was set; fall back to the evaluation threshold
            if r["quarantine_rate_pct"] > thresholds["quarantine_spike_pct"]:
                out.append(r)
    return out


def _missing_source_data(snap, _t):
    # a load that completed but landed zero rows = source delivered nothing
    return [r for r in snap["table_load_status"]
            if r["status"] != FAILED and r["records_written"] == 0]


def _freshness_breach(snap, _t):
    return [r for r in snap["table_freshness"] if r["sla_breached"]]


def _security_violation(snap, _t):
    return [r for r in snap["security_events"] if r["status"] == VIOLATION]


def _cost_breach(snap, _t):
    return [r for r in snap["cost_summary"] if r["budget_breached"]]


# --------------------------------------------------------------------------- #
# the rule set (one per required alert)
# --------------------------------------------------------------------------- #
ALERT_RULES = [
    {"id": "pipeline_failure", "title": "Pipeline run failed",
     "severity": CRITICAL, "channels": ["email", "teams", "itsm"],
     "predicate": _pipeline_failure},

    {"id": "bronze_validation_failure", "title": "Bronze validation gate failed",
     "severity": CRITICAL, "channels": ["email", "teams"],
     "predicate": _bronze_validation_failure},

    {"id": "silver_dq_failure", "title": "Silver data-quality gate failed",
     "severity": HIGH, "channels": ["email", "teams"],
     "predicate": _silver_dq_failure},

    {"id": "dbt_test_failure", "title": "dbt tests failed",
     "severity": HIGH, "channels": ["email", "teams"],
     "predicate": _dbt_test_failure},

    {"id": "quarantine_spike", "title": "Quarantine rate spike",
     "severity": HIGH, "channels": ["email", "teams"],
     "predicate": _quarantine_spike},

    {"id": "missing_source_data", "title": "Source delivered no data",
     "severity": HIGH, "channels": ["email", "teams"],
     "predicate": _missing_source_data},

    {"id": "table_freshness_sla", "title": "Table freshness SLA breached",
     "severity": HIGH, "channels": ["email", "teams"],
     "predicate": _freshness_breach},

    {"id": "pii_security_violation", "title": "PII / security policy violation",
     "severity": CRITICAL, "channels": ["email", "teams", "itsm"],
     "predicate": _security_violation},

    {"id": "cost_threshold_breach", "title": "Cost threshold breached",
     "severity": MEDIUM, "channels": ["email", "teams"],
     "predicate": _cost_breach},
]

ALERT_RULES_BY_ID = {r["id"]: r for r in ALERT_RULES}


def evaluate_alerts(monitor_or_snapshot, thresholds=None):
    """Return the list of fired alerts for a monitor (or its snapshot).

    Each fired alert: {id, title, severity, channels, count, offenders}.
    """
    snap = (monitor_or_snapshot.snapshot()
            if hasattr(monitor_or_snapshot, "snapshot") else monitor_or_snapshot)
    thresholds = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    fired = []
    for rule in ALERT_RULES:
        offenders = rule["predicate"](snap, thresholds)
        if offenders:
            fired.append({
                "id": rule["id"],
                "title": rule["title"],
                "severity": rule["severity"],
                "channels": list(rule["channels"]),
                "count": len(offenders),
                "offenders": offenders,
            })
    return fired


def triggered_ids(monitor_or_snapshot, thresholds=None):
    """Convenience: the set of rule ids that fired."""
    return {a["id"] for a in evaluate_alerts(monitor_or_snapshot, thresholds)}
