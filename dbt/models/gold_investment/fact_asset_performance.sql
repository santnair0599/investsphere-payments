-- Asset valuation / return facts (grain: asset_id, as_of_date).
{{ config(
    materialized='incremental',
    unique_key='performance_id',
    incremental_strategy='merge'
) }}

SELECT
    performance_id,
    asset_id,
    as_of_date,
    nav,
    mtd_return,
    ytd_return,
    benchmark_return,
    ytd_return - benchmark_return AS excess_return,
    source_system
FROM {{ source('silver_investment', 'asset_performance_clean') }}

{% if is_incremental() %}
  -- only reprocess the recent window (late-arriving snapshots included)
  WHERE as_of_date >= (SELECT COALESCE(MAX(as_of_date), DATE'1900-01-01') FROM {{ this }})
{% endif %}
