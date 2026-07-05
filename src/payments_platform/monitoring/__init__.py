"""
Monitoring & control layer (observability as policy-as-code).

Nine declarative models (:mod:`models`) capture every run's health; a pure-Python
:class:`recorder.RunMonitor` writes them deterministically (timestamps come from
the :class:`RunContext`, never the wall clock). :mod:`alerts` evaluates the
records against declarative alert rules; :mod:`dashboards` emits Databricks
SQL-ready dashboard queries; :mod:`instrument` wires the monitor into the
orchestration DAG so every task, gate, dbt run and governance check is recorded.

In production the models map 1:1 to Delta tables in ``silver_control.*`` (run
control) and ``monitoring.*`` (DQ / freshness / quarantine / dbt / security /
cost), fed by the same library here. See ``docs/MONITORING.md``.
"""
from payments_platform.monitoring.recorder import RunMonitor       # noqa: F401
from payments_platform.monitoring import models, alerts, dashboards  # noqa: F401
