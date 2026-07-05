-- Cost vs budget by environment and warehouse (last 30 days)
SELECT
  run_date, environment, warehouse,
  sum(dbus)                                                  AS dbus,
  round(sum(cost_usd), 2)                                    AS cost_usd,
  max(budget_usd)                                            AS budget_usd,
  max(CASE WHEN budget_breached THEN 1 ELSE 0 END)           AS breached
FROM investsphere.monitoring.cost_summary
WHERE run_date >= current_date() - INTERVAL 30 DAYS
GROUP BY run_date, environment, warehouse
ORDER BY run_date DESC, cost_usd DESC;

-- In production, reconcile against billing system tables:
-- SELECT usage_date, sku_name, sum(usage_quantity) AS dbus
-- FROM system.billing.usage
-- WHERE usage_metadata.job_name = 'investsphere_payments_daily_e2e'
-- GROUP BY usage_date, sku_name;
