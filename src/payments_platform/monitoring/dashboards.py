"""
Databricks SQL-ready dashboard queries (one per required dashboard).

Each query reads the monitoring/control Delta tables under the per-environment
catalog. ``generate_all(catalog)`` substitutes the catalog and returns
``{filename: sql}``; ``pipelines/generate_monitoring_sql.py`` writes them to
``monitoring/sql/`` (mirroring ``governance/sql/``).

In production these are the visual layer; the operational alerting equivalents
live in ``alerts.py`` and are forwarded to Azure Monitor (see docs/MONITORING.md).
The ``{catalog}`` placeholder is replaced per environment (investsphere_dev/…).
"""
from __future__ import annotations

DEFAULT_CATALOG = "investsphere"

# name -> (filename, sql template with {catalog})
_DASHBOARDS = {
    # 1. pipeline health: run success rate, durations, recent failures, retries
    "pipeline_health": ("pipeline_health.sql", """
-- Pipeline health: success rate, duration and retries by day
SELECT
  pr.run_date,
  pr.job_name,
  count(*)                                                   AS runs,
  sum(CASE WHEN pr.status = 'SUCCESS' THEN 1 ELSE 0 END)     AS succeeded,
  sum(CASE WHEN pr.status = 'FAILED'  THEN 1 ELSE 0 END)     AS failed,
  sum(CASE WHEN pr.status = 'PARTIAL' THEN 1 ELSE 0 END)     AS partial,
  round(100.0 * sum(CASE WHEN pr.status = 'SUCCESS' THEN 1 ELSE 0 END)
        / count(*), 1)                                       AS success_rate_pct,
  round(avg(unix_timestamp(pr.end_time) - unix_timestamp(pr.start_time)) / 60.0, 1)
                                                             AS avg_duration_min
FROM {catalog}.silver_control.pipeline_run pr
WHERE pr.run_date >= current_date() - INTERVAL 30 DAYS
GROUP BY pr.run_date, pr.job_name
ORDER BY pr.run_date DESC;

-- Tasks that retried (attempt > 1) or failed, last 7 days
SELECT tr.run_date, tr.run_id, tr.task_key, tr.phase, tr.status,
       max(tr.attempt) AS attempts
FROM {catalog}.silver_control.task_run tr
WHERE tr.run_date >= current_date() - INTERVAL 7 DAYS
  AND (tr.attempt > 1 OR tr.status = 'FAILED')
GROUP BY tr.run_date, tr.run_id, tr.task_key, tr.phase, tr.status
ORDER BY tr.run_date DESC, attempts DESC;
"""),

    # 2. data quality: pass/fail by rule and entity
    "data_quality": ("data_quality.sql", """
-- DQ outcomes by entity and rule (last 14 days)
SELECT
  run_date, entity, rule_name, severity, action,
  sum(records_evaluated)                                     AS evaluated,
  sum(records_failed)                                        AS failed,
  round(100.0 * sum(records_failed) / nullif(sum(records_evaluated), 0), 2)
                                                             AS fail_rate_pct,
  sum(CASE WHEN result = 'FAIL' THEN 1 ELSE 0 END)           AS fail_events
FROM {catalog}.monitoring.dq_results
WHERE run_date >= current_date() - INTERVAL 14 DAYS
GROUP BY run_date, entity, rule_name, severity, action
ORDER BY run_date DESC, failed DESC;
"""),

    # 3. freshness: lag vs SLA, current breaches
    "freshness": ("freshness.sql", """
-- Latest freshness per table with SLA breach flag
WITH latest AS (
  SELECT table_name, max(event_time) AS event_time
  FROM {catalog}.monitoring.table_freshness
  GROUP BY table_name
)
SELECT f.table_name, f.last_loaded_at, f.as_of,
       f.lag_minutes, f.sla_minutes, f.sla_breached
FROM {catalog}.monitoring.table_freshness f
JOIN latest USING (table_name, event_time)
ORDER BY f.sla_breached DESC, f.lag_minutes DESC;
"""),

    # 4. quarantine: rate trend and spikes
    "quarantine": ("quarantine.sql", """
-- Quarantine rate trend + spike flags by entity (last 30 days)
SELECT
  run_date, entity, source_table,
  sum(records_total)                                         AS total,
  sum(records_quarantined)                                   AS quarantined,
  round(100.0 * sum(records_quarantined) / nullif(sum(records_total), 0), 2)
                                                             AS quarantine_rate_pct,
  max(CASE WHEN is_spike THEN 1 ELSE 0 END)                  AS spiked
FROM {catalog}.monitoring.quarantine_summary
WHERE run_date >= current_date() - INTERVAL 30 DAYS
GROUP BY run_date, entity, source_table
ORDER BY run_date DESC, quarantine_rate_pct DESC;
"""),

    # 5. security / PII access: governance violations + (prod) UC audit
    "security_pii": ("security_pii.sql", """
-- Governance/PII policy validation events (violations first)
SELECT run_date, check_name, severity, status, violation_count, violations
FROM {catalog}.monitoring.security_events
WHERE run_date >= current_date() - INTERVAL 30 DAYS
ORDER BY (status = 'VIOLATION') DESC, run_date DESC;

-- In production, join actual PII access from the UC system audit table:
-- SELECT event_time, user_identity.email, action_name, request_params.full_name_arg
-- FROM system.access.audit
-- WHERE service_name = 'unityCatalog'
--   AND request_params.full_name_arg LIKE '{catalog}.silver_cdc.%'
-- ORDER BY event_time DESC;
"""),

    # 6. cost: spend vs budget by env/warehouse
    "cost": ("cost.sql", """
-- Cost vs budget by environment and warehouse (last 30 days)
SELECT
  run_date, environment, warehouse,
  sum(dbus)                                                  AS dbus,
  round(sum(cost_usd), 2)                                    AS cost_usd,
  max(budget_usd)                                            AS budget_usd,
  max(CASE WHEN budget_breached THEN 1 ELSE 0 END)           AS breached
FROM {catalog}.monitoring.cost_summary
WHERE run_date >= current_date() - INTERVAL 30 DAYS
GROUP BY run_date, environment, warehouse
ORDER BY run_date DESC, cost_usd DESC;

-- In production, reconcile against billing system tables:
-- SELECT usage_date, sku_name, sum(usage_quantity) AS dbus
-- FROM system.billing.usage
-- WHERE usage_metadata.job_name = 'investsphere_payments_daily_e2e'
-- GROUP BY usage_date, sku_name;
"""),
}

DASHBOARD_NAMES = list(_DASHBOARDS)


def dashboard_sql(name, catalog=DEFAULT_CATALOG):
    """The SQL for one dashboard, with the catalog substituted."""
    _, template = _DASHBOARDS[name]
    return template.replace("{catalog}", catalog).strip() + "\n"


def generate_all(catalog=DEFAULT_CATALOG):
    """Return ``{filename: sql}`` for every dashboard."""
    return {
        filename: template.replace("{catalog}", catalog).strip() + "\n"
        for filename, template in _DASHBOARDS.values()
    }
