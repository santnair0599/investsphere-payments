-- Lease fact: one row per lease agreement (rent roll grain).
{{ config(materialized='table') }}

SELECT
    lease_id,
    property_id,
    customer_id,
    unit_id,
    monthly_rent,
    currency_code,
    lease_start,
    lease_end,
    status,
    source_system
FROM {{ source('silver_realestate', 'lease_clean') }}
