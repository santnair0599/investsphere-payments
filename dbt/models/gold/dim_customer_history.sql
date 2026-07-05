-- Full SCD2 history dimension for point-in-time / as-of reporting.
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_name,
    email,
    phone_number,
    nationality,
    status,
    source_system,
    effective_from,
    effective_to,
    is_current,
    is_deleted
FROM {{ source('silver_cdc', 'customer_scd2') }}
