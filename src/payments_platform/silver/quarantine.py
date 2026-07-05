"""
Silver quarantine: split valid vs invalid, never silently drop.

Covers the spec's Silver step 4: failed records go to a Failed Record Register
with full context (failure reason, rule, severity, raw payload, lineage, status).
A FAIL-severity breach raises (the job stops); QUARANTINE-severity rows are
routed out and processing continues.
"""
from __future__ import annotations

from payments_platform.silver import dq

# resolution lifecycle for a quarantined record
OPEN = "OPEN"


def split(rows, rules, ctx, source_table, record_key="payment_id"):
    """Split ``rows`` into (valid, quarantined).

    Returns:
        valid:        rows that passed every rule.
        quarantined:  one register entry per failing row, with quarantine
                      columns populated and status=OPEN.

    Raises:
        dq.MandatoryFieldError: if any row breaks a FAIL-severity rule.
    """
    valid, quarantined = [], []
    for row in rows:
        failures = dq.evaluate(row, rules)
        if not failures:
            valid.append(row)
            continue

        # a FAIL-severity breach stops the whole job (unauditable data)
        fail_breaches = [f for f in failures if f["severity"] == dq.FAIL]
        if fail_breaches:
            raise dq.MandatoryFieldError(
                "FAIL-severity rule(s) on "
                + str(row.get(record_key))
                + ": " + "; ".join(f["reason"] for f in fail_breaches)
            )

        quarantined.append(_register_entry(
            row, failures, ctx, source_table, record_key))
    return valid, quarantined


def _register_entry(row, failures, ctx, source_table, record_key):
    """Build one Failed Record Register row."""
    # worst severity present (only QUARANTINE/WARN reach here)
    severities = {f["severity"] for f in failures}
    severity = dq.QUARANTINE if dq.QUARANTINE in severities else dq.WARN
    return {
        "source_system": ctx.source_system,
        "source_table": source_table,
        "record_key": row.get(record_key),
        "failure_reason": "; ".join(f["reason"] for f in failures),
        "failed_rule_name": ",".join(f["rule"] for f in failures),
        "severity": severity,
        "raw_payload": str(row),
        "ingestion_timestamp": row.get("ingestion_timestamp"),
        "failed_at": ctx.ingestion_timestamp,
        "run_id": ctx.run_id,
        "status": OPEN,
    }
