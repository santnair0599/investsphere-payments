-- Footfall fact (grain: footfall_id). Incremental on footfall_date.
{{ config(
    materialized='incremental',
    unique_key='footfall_id',
    incremental_strategy='merge'
) }}

SELECT
    footfall_id,
    venue_id,
    gate,
    visitors,
    footfall_date,
    source_system
FROM {{ source('silver_entertainment', 'footfall_clean') }}

{% if is_incremental() %}
WHERE footfall_date >= (SELECT COALESCE(MAX(footfall_date), DATE'1900-01-01') FROM {{ this }}) - INTERVAL 3 DAYS
{% endif %}
