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
FROM investsphere.silver_control.pipeline_run pr
WHERE pr.run_date >= current_date() - INTERVAL 30 DAYS
GROUP BY pr.run_date, pr.job_name
ORDER BY pr.run_date DESC;

-- Tasks that retried (attempt > 1) or failed, last 7 days
SELECT tr.run_date, tr.run_id, tr.task_key, tr.phase, tr.status,
       max(tr.attempt) AS attempts
FROM investsphere.silver_control.task_run tr
WHERE tr.run_date >= current_date() - INTERVAL 7 DAYS
  AND (tr.attempt > 1 OR tr.status = 'FAILED')
GROUP BY tr.run_date, tr.run_id, tr.task_key, tr.phase, tr.status
ORDER BY tr.run_date DESC, attempts DESC;
