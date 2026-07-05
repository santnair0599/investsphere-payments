-- Analysts read the BI serving views only (not the Gold base).
GRANT SELECT ON VIEW investsphere.gold_marts.v_payments_daily_bi TO `analysts`;
GRANT SELECT ON VIEW investsphere.gold_marts.v_payments_fact_bi TO `analysts`;
GRANT SELECT ON VIEW investsphere.gold_marts.v_customer_bi TO `analysts`;
