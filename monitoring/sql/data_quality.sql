-- DQ outcomes by entity and rule (last 14 days)
SELECT
  run_date, entity, rule_name, severity, action,
  sum(records_evaluated)                                     AS evaluated,
  sum(records_failed)                                        AS failed,
  round(100.0 * sum(records_failed) / nullif(sum(records_evaluated), 0), 2)
                                                             AS fail_rate_pct,
  sum(CASE WHEN result = 'FAIL' THEN 1 ELSE 0 END)           AS fail_events
FROM investsphere.monitoring.dq_results
WHERE run_date >= current_date() - INTERVAL 14 DAYS
GROUP BY run_date, entity, rule_name, severity, action
ORDER BY run_date DESC, failed DESC;
