"""
Quality gates between medallion layers (pure, reusable).

A gate returns ``(passed: bool, reasons: list)``. In Databricks the boolean is
written as a task value and a **condition task** branches on it: pass -> run
downstream; fail -> stop and alert.
"""
from __future__ import annotations

DEFAULT_BRONZE_POLICY = {
    "required_sources": [],     # source keys that must have loaded SUCCESS
    "max_corrupt_records": 50,  # total corrupt rows allowed
    "min_total_rows": 1,        # at least this many rows ingested
}

DEFAULT_SILVER_POLICY = {
    "max_quarantine_rate_pct": 30.0,   # quarantine share ceiling
    "block_on_fail_severity": True,    # any FAIL-severity breach blocks
}


def bronze_validation_gate(bronze_results, policy=None):
    """Decide whether Bronze is healthy enough to promote to Silver.

    Args:
        bronze_results: list of per-source dicts with keys: source (str),
            status (SUCCESS/FAILED/SKIPPED), records_written (int),
            corrupt_records (int, optional).
    """
    p = {**DEFAULT_BRONZE_POLICY, **(policy or {})}
    reasons = []
    by_source = {r["source"]: r for r in bronze_results}

    # 1. required sources loaded successfully
    for src in p["required_sources"]:
        res = by_source.get(src)
        if res is None:
            reasons.append("required source missing: " + src)
        elif res["status"] != "SUCCESS":
            reasons.append("required source not SUCCESS: %s (%s)"
                           % (src, res["status"]))

    # 2. no failed ingestion among any source
    for r in bronze_results:
        if r["status"] == "FAILED":
            reasons.append("ingestion failed: " + r["source"])

    # 3. corrupt-record threshold
    total_corrupt = sum(r.get("corrupt_records", 0) for r in bronze_results)
    if total_corrupt > p["max_corrupt_records"]:
        reasons.append("corrupt records %d > threshold %d"
                       % (total_corrupt, p["max_corrupt_records"]))

    # 4. row-count sanity
    total_rows = sum(r.get("records_written", 0) for r in bronze_results)
    if total_rows < p["min_total_rows"]:
        reasons.append("total rows %d < min %d" % (total_rows, p["min_total_rows"]))

    return (len(reasons) == 0, reasons)


def silver_dq_gate(silver_results, policy=None):
    """Decide whether Silver is trustworthy enough to promote to dbt Gold.

    Args:
        silver_results: list of dicts with keys: entity, valid (int),
            quarantined (int), fail_severity (bool, optional).
    """
    p = {**DEFAULT_SILVER_POLICY, **(policy or {})}
    reasons = []
    for r in silver_results:
        if p["block_on_fail_severity"] and r.get("fail_severity"):
            reasons.append("FAIL-severity breach in " + r["entity"])
        total = r.get("valid", 0) + r.get("quarantined", 0)
        if total:
            rate = r["quarantined"] / total * 100
            if rate > p["max_quarantine_rate_pct"]:
                reasons.append("%s quarantine rate %.1f%% > %.1f%%"
                               % (r["entity"], rate, p["max_quarantine_rate_pct"]))
    return (len(reasons) == 0, reasons)
