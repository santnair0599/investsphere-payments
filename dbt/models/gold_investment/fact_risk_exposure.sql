-- Asset risk exposure facts (grain: asset_id, as_of_date).
{{ config(
    materialized='incremental',
    unique_key='exposure_id',
    incremental_strategy='merge'
) }}

SELECT
    exposure_id,
    asset_id,
    as_of_date,
    var_95,
    volatility,
    concentration_pct,
    risk_rating,
    risk_threshold_breached,
    source_system
FROM {{ source('silver_investment', 'risk_exposure_clean') }}

{% if is_incremental() %}
  WHERE as_of_date >= (SELECT COALESCE(MAX(as_of_date), DATE'1900-01-01') FROM {{ this }})
{% endif %}
