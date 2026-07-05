"""
Declarative DAG for ``investsphere_payments_daily_e2e``.

Each task: key, kind (python/condition/dbt), phase, depends_on. A dependency is
either a task key (satisfied when that task succeeds) or {"task", "outcome"} for
a gate (satisfied only when the gate returns that boolean outcome).

`enabled=False` tasks are future sources — present in the model but not run.
REST / SFTP / Salesforce are now **active** Bronze sources (they fan out in
parallel with the other Bronze tasks). They are deliberately NOT dependencies of
``bronze_validation_gate``: a failure in one of them does not block the gate or
downstream unless it is added to the gate's ``required_sources`` policy.
"""
from __future__ import annotations

PYTHON, CONDITION, DBT = "python", "condition", "dbt"

TASKS = [
    {"key": "init_run", "kind": PYTHON, "phase": "init", "depends_on": []},

    # ---- Bronze (parallel: all depend only on init_run) ----
    {"key": "bronze_payments_file", "kind": PYTHON, "phase": "bronze",
     "depends_on": ["init_run"]},
    {"key": "bronze_jdbc", "kind": PYTHON, "phase": "bronze",
     "depends_on": ["init_run"]},
    {"key": "bronze_customer_cdc", "kind": PYTHON, "phase": "bronze",
     "depends_on": ["init_run"]},
    # additional sources (now active; fan out in parallel, not gate dependencies)
    {"key": "bronze_rest_api", "kind": PYTHON, "phase": "bronze",
     "depends_on": ["init_run"]},
    {"key": "bronze_sftp", "kind": PYTHON, "phase": "bronze",
     "depends_on": ["init_run"]},
    {"key": "bronze_salesforce", "kind": PYTHON, "phase": "bronze",
     "depends_on": ["init_run"]},

    # ---- Bronze validation gate ----
    {"key": "bronze_validation_gate", "kind": CONDITION, "phase": "gate",
     "depends_on": ["bronze_payments_file", "bronze_jdbc", "bronze_customer_cdc"]},

    # ---- Silver (only if Bronze gate passed) ----
    {"key": "silver_payments", "kind": PYTHON, "phase": "silver",
     "depends_on": [{"task": "bronze_validation_gate", "outcome": True}]},
    {"key": "silver_customer_scd2", "kind": PYTHON, "phase": "silver",
     "depends_on": [{"task": "bronze_validation_gate", "outcome": True}]},

    # ---- Silver DQ gate ----
    {"key": "silver_dq_gate", "kind": CONDITION, "phase": "gate",
     "depends_on": ["silver_payments", "silver_customer_scd2"]},

    # ---- dbt Gold (only if Silver DQ gate passed) ----
    {"key": "dbt_build", "kind": DBT, "phase": "gold",
     "depends_on": [{"task": "silver_dq_gate", "outcome": True}]},
    {"key": "dbt_test", "kind": DBT, "phase": "gold",
     "depends_on": ["dbt_build"]},

    # ---- governance validation (after dbt) ----
    {"key": "governance_validation", "kind": PYTHON, "phase": "governance",
     "depends_on": ["dbt_test"]},

    # ---- publish / notify ----
    {"key": "publish_notify", "kind": PYTHON, "phase": "publish",
     "depends_on": ["governance_validation"]},
]


def enabled_tasks(tasks=None):
    tasks = TASKS if tasks is None else tasks
    return [t for t in tasks if t.get("enabled", True)]


def get_task(key, tasks=None):
    tasks = TASKS if tasks is None else tasks
    return next(t for t in tasks if t["key"] == key)


def dependencies(task):
    """Normalise depends_on to a list of (task_key, required_outcome|None)."""
    out = []
    for dep in task.get("depends_on", []):
        if isinstance(dep, dict):
            out.append((dep["task"], dep.get("outcome")))
        else:
            out.append((dep, None))
    return out


def topo_order(tasks=None):
    """Deterministic topological order over enabled tasks."""
    tasks = enabled_tasks(tasks)
    keys = {t["key"] for t in tasks}
    remaining = {t["key"]: [d for d, _ in dependencies(t) if d in keys] for t in tasks}
    order = []
    while remaining:
        ready = sorted(k for k, deps in remaining.items()
                       if all(d in order for d in deps))
        if not ready:
            raise ValueError("cycle or missing dependency in DAG: " + str(remaining))
        order.extend(ready)
        for k in ready:
            del remaining[k]
    return order


def parallel_groups(tasks=None):
    """Levels of tasks that can run concurrently (no intra-level dependency)."""
    tasks = enabled_tasks(tasks)
    keys = {t["key"] for t in tasks}
    remaining = {t["key"]: {d for d, _ in dependencies(t) if d in keys} for t in tasks}
    done, groups = set(), []
    while remaining:
        level = sorted(k for k, deps in remaining.items() if deps <= done)
        if not level:
            raise ValueError("cycle in DAG")
        groups.append(level)
        done |= set(level)
        for k in level:
            del remaining[k]
    return groups


def downstream(key, tasks=None):
    """All tasks transitively depending on ``key``."""
    tasks = enabled_tasks(tasks)
    result, frontier = set(), [key]
    while frontier:
        cur = frontier.pop()
        for t in tasks:
            if cur in [d for d, _ in dependencies(t)] and t["key"] not in result:
                result.add(t["key"])
                frontier.append(t["key"])
    return result
