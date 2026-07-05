-- Current customer (from SCD2: latest version, not deleted).
{{ config(materialized='view') }}

SELECT
    customer_id,
    customer_name,
    email,
    phone_number,
    nationality,
    status,
    source_system,
    effective_from
FROM {{ source('silver_cdc', 'customer_scd2') }}
WHERE is_current = true
  AND is_deleted = false
