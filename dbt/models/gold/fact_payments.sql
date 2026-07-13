-- Payments fact — grain: one row per payment_id.
-- Measures (amount) + dimension foreign keys (customer_id, account_id) drawn
-- from the DQ-conformed Silver payments table. Join to dim_customer for the
-- current customer, or dim_customer_history for an as-of (point-in-time) view.
{{ config(materialized='table') }}

SELECT
    payment_id,
    customer_id,          -- FK -> dim_customer / dim_customer_history
    account_id,
    amount,
    currency_code,
    payment_type,
    transaction_date,
    source_system
FROM {{ source('silver_clean', 'payment_clean') }}
