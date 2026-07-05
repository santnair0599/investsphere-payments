-- Quarantine rate trend + spike flags by entity (last 30 days)
SELECT
  run_date, entity, source_table,
  sum(records_total)                                         AS total,
  sum(records_quarantined)                                   AS quarantined,
  round(100.0 * sum(records_quarantined) / nullif(sum(records_total), 0), 2)
                                                             AS quarantine_rate_pct,
  max(CASE WHEN is_spike THEN 1 ELSE 0 END)                  AS spiked
FROM investsphere.monitoring.quarantine_summary
WHERE run_date >= current_date() - INTERVAL 30 DAYS
GROUP BY run_date, entity, source_table
ORDER BY run_date DESC, quarantine_rate_pct DESC;
