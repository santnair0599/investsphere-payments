-- Venue dimension (one row per venue_id) from the conformed Silver master.
{{ config(materialized='table') }}

SELECT
    venue_id,
    venue_name,
    venue_type,
    city,
    emirate,
    capacity,
    source_system
FROM {{ source('silver_entertainment', 'venue_clean') }}
