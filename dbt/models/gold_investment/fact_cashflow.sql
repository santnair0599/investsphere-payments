-- Asset cashflow facts (grain: cashflow_id).
{{ config(materialized='table') }}

SELECT
    cashflow_id,
    asset_id,
    value_date,
    cashflow_type,
    amount,
    currency_code,
    source_system
FROM {{ source('silver_investment', 'cashflow_clean') }}
