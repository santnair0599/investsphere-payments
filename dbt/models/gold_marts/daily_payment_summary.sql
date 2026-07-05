-- Dashboard-ready daily revenue by currency/type, enriched with customer country.
{{ config(materialized='table') }}

SELECT
    f.transaction_date,
    f.currency_code,
    f.payment_type,
    c.nationality              AS customer_country,
    COUNT(*)                   AS payment_count,
    SUM(f.amount)              AS total_amount,
    AVG(f.amount)              AS avg_amount
FROM {{ ref('fact_payments') }} f
LEFT JOIN {{ ref('dim_customer') }} c
    ON f.customer_id = c.customer_id
GROUP BY
    f.transaction_date,
    f.currency_code,
    f.payment_type,
    c.nationality
