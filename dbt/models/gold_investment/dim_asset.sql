-- Investment asset dimension (one row per asset_id).
{{ config(materialized='table') }}

SELECT
    asset_id,
    asset_name,
    asset_class,
    sector,
    currency_code,
    inception_date,
    source_system
FROM {{ source('silver_investment', 'asset_clean') }}
