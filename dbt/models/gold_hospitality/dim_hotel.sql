-- Hotel dimension (one row per hotel_id) from the conformed Silver hotel master.
{{ config(materialized='table') }}

SELECT
    hotel_id,
    hotel_name,
    city,
    emirate,
    star_rating,
    rooms_total,
    brand,
    source_system
FROM {{ source('silver_hospitality', 'hotel_clean') }}
