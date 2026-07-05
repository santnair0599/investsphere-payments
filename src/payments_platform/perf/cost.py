"""
Cost-observability reference.

Estimates spend from a task's duration + compute type, attributes it by task /
source / layer / environment, and flags expensive tasks, repeated failed-run
cost, and long-running regressions. The estimates feed ``monitoring.cost_summary``
(via :meth:`RunMonitor.record_cost`) so the existing ``cost_threshold_breach``
alert can fire.

The DBU rates here are reference constants — in production the real numbers come
from ``system.billing.usage`` joined to job/warehouse tags (see
docs/PERFORMANCE_COST.md). The point is the *attribution model*, not the price.
"""
from __future__ import annotations

# reference $/DBU and DBU/hour by compute type (illustrative, not a price list)
DBU_PER_HOUR = {
    "serverless_job": 4.0,
    "job_cluster": 6.0,
    "all_purpose": 8.0,
    "sql_serverless": 12.0,
    "sql_pro": 10.0,
}
USD_PER_DBU = {
    "serverless_job": 0.35,
    "job_cluster": 0.30,
    "all_purpose": 0.55,   # all-purpose is the most expensive per DBU
    "sql_serverless": 0.70,
    "sql_pro": 0.55,
}

# which medallion layer each DAG task belongs to (cost attribution)
TASK_LAYER = {
    "bronze_payments_file": "bronze", "bronze_jdbc": "bronze",
    "bronze_customer_cdc": "bronze", "bronze_rest_api": "bronze",
    "bronze_sftp": "bronze", "bronze_salesforce": "bronze",
    "silver_payments": "silver", "silver_customer_scd2": "silver",
    "dbt_build": "gold", "dbt_test": "gold",
    "governance_validation": "governance", "publish_notify": "publish",
}

DEFAULT_EXPENSIVE_USD = 5.0


def estimate_cost(duration_ms, compute_type="serverless_job"):
    """Return (dbus, cost_usd) for a task of ``duration_ms`` on ``compute_type``."""
    hours = max(0.0, duration_ms) / 3_600_000.0
    dbu_rate = DBU_PER_HOUR.get(compute_type, DBU_PER_HOUR["serverless_job"])
    usd_rate = USD_PER_DBU.get(compute_type, USD_PER_DBU["serverless_job"])
    dbus = hours * dbu_rate
    return round(dbus, 6), round(dbus * usd_rate, 6)


def cost_for_task(task, duration_ms, compute_type="serverless_job",
                  source=None, env="dev"):
    """A per-task cost row attributed by task / source / layer / environment."""
    dbus, cost_usd = estimate_cost(duration_ms, compute_type)
    return {
        "task": task,
        "source": source,
        "layer": TASK_LAYER.get(task, "other"),
        "environment": env,
        "compute_type": compute_type,
        "duration_ms": duration_ms,
        "dbus": dbus,
        "cost_usd": cost_usd,
    }


def estimate_costs(task_durations, compute_type="serverless_job", env="dev"):
    """Build cost rows for ``{task: duration_ms}`` (or a list of (task, ms))."""
    items = (task_durations.items() if isinstance(task_durations, dict)
             else task_durations)
    return [cost_for_task(t, ms, compute_type=compute_type, env=env)
            for t, ms in items]


def aggregate_cost(cost_rows, by="layer"):
    """Sum cost_usd grouped by one of: task / source / layer / environment."""
    out = {}
    for r in cost_rows:
        out[r.get(by)] = round(out.get(r.get(by), 0.0) + r["cost_usd"], 6)
    return out


def total_cost(cost_rows):
    return round(sum(r["cost_usd"] for r in cost_rows), 6)


# --------------------------------------------------------------------------- #
# flags
# --------------------------------------------------------------------------- #
def flag_expensive_tasks(cost_rows, threshold_usd=DEFAULT_EXPENSIVE_USD):
    """Tasks whose estimated cost exceeds ``threshold_usd``."""
    return [r for r in cost_rows if r["cost_usd"] > threshold_usd]


def flag_repeated_failed_run_cost(run_history, max_failures=3):
    """Flag wasted spend from repeated FAILED runs.

    ``run_history`` = list of {status, cost_usd}. Returns a dict with the failed
    count, the summed wasted cost, and whether it breached ``max_failures``."""
    failed = [r for r in run_history if r.get("status") == "FAILED"]
    wasted = round(sum(r.get("cost_usd", 0.0) for r in failed), 6)
    return {
        "failed_runs": len(failed),
        "wasted_cost_usd": wasted,
        "breached": len(failed) >= max_failures,
    }


def flag_long_running_regression(current_ms, baseline_ms, pct=50.0):
    """True if ``current_ms`` regressed more than ``pct`` over ``baseline_ms``."""
    if baseline_ms <= 0:
        return False
    return (current_ms - baseline_ms) / baseline_ms * 100.0 > pct


def regressions(current, baseline, pct=50.0):
    """Tasks in ``current`` ({task: ms}) that regressed > ``pct`` vs ``baseline``."""
    out = []
    for task, ms in current.items():
        base = baseline.get(task)
        if base is not None and flag_long_running_regression(ms, base, pct):
            out.append({"task": task, "baseline_ms": base, "current_ms": ms,
                        "pct": round((ms - base) / base * 100.0, 1)})
    return out


def record_costs(monitor, cost_rows, budget_usd=None, threshold_pct=80.0):
    """Write each cost row to monitoring.cost_summary via the monitor (so the
    cost_threshold_breach alert can fire). Returns the written rows."""
    written = []
    for r in cost_rows:
        written.append(monitor.record_cost(
            warehouse="%s:%s" % (r["compute_type"], r["task"]),
            dbus=r["dbus"], cost_usd=r["cost_usd"],
            budget_usd=budget_usd, threshold_pct=threshold_pct))
    return written
