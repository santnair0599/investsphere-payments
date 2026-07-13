-- Maintenance fact: one row per work order (cost + status grain).
{{ config(materialized='table') }}

SELECT
    work_order_id,
    property_id,
    category,
    priority,
    cost,
    currency_code,
    opened_date,
    closed_date,
    status,
    source_system
FROM {{ source('silver_realestate', 'maintenance_clean') }}
