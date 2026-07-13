-- Business surface: daily payment volume + value by currency and payment type.
-- The kind of aggregate a BI dashboard or an AI SQL tool queries directly.
{{ config(materialized='table') }}

SELECT
    transaction_date,
    currency_code,
    payment_type,
    count(*)      AS payment_count,
    sum(amount)   AS total_amount,
    avg(amount)   AS avg_amount
FROM {{ ref('fact_payments') }}
GROUP BY transaction_date, currency_code, payment_type
