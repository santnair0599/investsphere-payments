-- Daily payments BI dataset (passthrough of the Gold mart; no PII)
CREATE OR REPLACE VIEW investsphere.gold_marts.v_payments_daily_bi AS
SELECT transaction_date, currency_code, payment_type, customer_country,
       payment_count, total_amount, avg_amount
FROM investsphere.gold_marts.daily_payment_summary;
