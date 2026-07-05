-- Customer dimension for BI: sources the masked view (PII display-masked)
CREATE OR REPLACE VIEW investsphere.gold_marts.v_customer_bi AS
SELECT customer_id, nationality, status, source_system,
       customer_name, email, phone_number
FROM investsphere.gold_masked.v_customer_masked_for_analytics;
