-- Thin staging view over trusted Silver payments. Only DQ-passed rows flow on.
{{ config(materialized='view') }}

SELECT
    payment_id,
    customer_id,
    account_id,
    amount,
    currency_code,
    payment_type,
    transaction_date,
    source_system,
    ingestion_timestamp
FROM {{ source('silver_clean', 'payment_clean') }}
