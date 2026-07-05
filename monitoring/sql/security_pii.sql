-- Governance/PII policy validation events (violations first)
SELECT run_date, check_name, severity, status, violation_count, violations
FROM investsphere.monitoring.security_events
WHERE run_date >= current_date() - INTERVAL 30 DAYS
ORDER BY (status = 'VIOLATION') DESC, run_date DESC;

-- In production, join actual PII access from the UC system audit table:
-- SELECT event_time, user_identity.email, action_name, request_params.full_name_arg
-- FROM system.access.audit
-- WHERE service_name = 'unityCatalog'
--   AND request_params.full_name_arg LIKE 'investsphere.silver_cdc.%'
-- ORDER BY event_time DESC;
