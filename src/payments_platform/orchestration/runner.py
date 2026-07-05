"""
Local DAG runner — executes the orchestration graph honouring dependencies and
gate outcomes, so the workflow logic is testable without a Databricks workspace.

A handler is ``fn(params, results) -> TaskResult``. Gate (condition) handlers set
``outcome`` True/False; downstream tasks that require ``outcome=True`` are SKIPPED
when the gate fails — mirroring a Lakeflow condition task.
"""
from __future__ import annotations

from payments_platform.orchestration import dag as D

SUCCESS, FAILED, SKIPPED = "SUCCESS", "FAILED", "SKIPPED"


class TaskResult:
    def __init__(self, status=SUCCESS, outcome=None, info=None, records_written=0):
        self.status = status
        self.outcome = outcome          # for gates: True/False
        self.info = info
        self.records_written = records_written

    def __repr__(self):
        return "TaskResult(%s, outcome=%s)" % (self.status, self.outcome)


def _deps_satisfied(task, results):
    """Return (runnable, reason). A task is blocked if any dependency failed,
    was skipped, or a gate didn't produce the required outcome."""
    for dep_key, needed in D.dependencies(task):
        dep = results.get(dep_key)
        if dep is None or dep.status in (FAILED, SKIPPED):
            return False, "upstream %s not satisfied" % dep_key
        if needed is not None and dep.outcome != needed:
            return False, "gate %s outcome=%s (needed %s)" % (
                dep_key, dep.outcome, needed)
    return True, None


def run_dag(handlers, params=None, tasks=None):
    """Execute the DAG. Returns {task_key: TaskResult} for enabled tasks.

    Tasks run in topological order (the runner is sequential, but the DAG's
    parallel structure is preserved — see ``dag.parallel_groups``). A failure or
    a failed gate blocks everything downstream.
    """
    params = params or {}
    enabled = D.enabled_tasks(tasks)
    results = {}
    for key in D.topo_order(enabled):
        task = D.get_task(key, enabled)
        runnable, reason = _deps_satisfied(task, results)
        if not runnable:
            results[key] = TaskResult(SKIPPED, info=reason)
            continue
        handler = handlers.get(key)
        if handler is None:
            results[key] = TaskResult(SUCCESS, outcome=(True if task["kind"] == D.CONDITION else None),
                                      info="no-op handler")
            continue
        try:
            res = handler(params, results)
            results[key] = res if isinstance(res, TaskResult) else TaskResult(info=res)
        except Exception as exc:  # noqa: BLE001
            results[key] = TaskResult(FAILED, info=str(exc))
    return results


def ran(results):
    """Keys that actually executed (SUCCESS or FAILED, not SKIPPED)."""
    return {k for k, r in results.items() if r.status in (SUCCESS, FAILED)}
