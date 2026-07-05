-- Payment-grain BI fact (no PII), enriched with customer_country for RLS
CREATE OR REPLACE VIEW investsphere.gold_marts.v_payments_fact_bi AS
SELECT f.payment_id, f.customer_id, f.account_id, f.amount,
       f.currency_code, f.payment_type, f.transaction_date,
       c.nationality AS customer_country, f.source_system
FROM investsphere.gold.fact_payments f
LEFT JOIN investsphere.gold.dim_customer c ON f.customer_id = c.customer_id;
