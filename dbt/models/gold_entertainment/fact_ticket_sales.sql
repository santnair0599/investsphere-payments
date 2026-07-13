-- Ticket sales fact (grain: ticket_id). Incremental on sale_date so daily runs
-- only reprocess the recent window; unique_key upserts late-arriving corrections.
{{ config(
    materialized='incremental',
    unique_key='ticket_id',
    incremental_strategy='merge'
) }}

SELECT
    ticket_id,
    venue_id,
    event_id,
    quantity,
    amount,
    currency_code,
    sale_date,
    source_system
FROM {{ source('silver_entertainment', 'ticket_sales_clean') }}

{% if is_incremental() %}
WHERE sale_date >= (SELECT COALESCE(MAX(sale_date), DATE'1900-01-01') FROM {{ this }}) - INTERVAL 3 DAYS
{% endif %}
