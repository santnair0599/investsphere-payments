-- Current customer dimension (SCD1-style view of SCD2 current rows).
{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_name,
    email,
    phone_number,
    nationality,
    status,
    source_system
FROM {{ ref('stg_customer') }}
