-- Payment fact, incremental on transaction_date (large-table pattern).
{{ config(materialized='incremental', unique_key='payment_id') }}

SELECT
    p.payment_id,
    p.customer_id,
    p.account_id,
    p.amount,
    p.currency_code,
    p.payment_type,
    p.transaction_date,
    p.source_system,
    current_timestamp() AS gold_processed_timestamp
FROM {{ ref('stg_payments') }} p

{% if is_incremental() %}
-- only reprocess a recent window on incremental runs
WHERE p.transaction_date >= date_sub(current_date(), 3)
{% endif %}
