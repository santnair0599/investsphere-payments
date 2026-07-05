-- Latest freshness per table with SLA breach flag
WITH latest AS (
  SELECT table_name, max(event_time) AS event_time
  FROM investsphere.monitoring.table_freshness
  GROUP BY table_name
)
SELECT f.table_name, f.last_loaded_at, f.as_of,
       f.lag_minutes, f.sla_minutes, f.sla_breached
FROM investsphere.monitoring.table_freshness f
JOIN latest USING (table_name, event_time)
ORDER BY f.sla_breached DESC, f.lag_minutes DESC;
