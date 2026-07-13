-- Daily hotel revenue fact (grain: revenue_id). Incremental on revenue_date so
-- each run only reprocesses the recent window; unique_key upserts on re-run.
{{ config(
    materialized='incremental',
    unique_key='revenue_id',
    incremental_strategy='merge'
) }}

SELECT
    revenue_id,
    hotel_id,
    revenue_date,
    rooms_available,
    rooms_sold,
    room_revenue,
    fnb_revenue,
    currency_code,
    revpar,
    occupancy_rate,
    source_system
FROM {{ source('silver_hospitality', 'revenue_clean') }}

{% if is_incremental() %}
-- only bring in rows newer than what we've already loaded
WHERE revenue_date > (SELECT COALESCE(MAX(revenue_date), DATE'1900-01-01') FROM {{ this }})
{% endif %}
