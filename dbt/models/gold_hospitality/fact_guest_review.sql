-- Guest review fact (one row per review_id) from conformed Silver reviews.
{{ config(materialized='table') }}

SELECT
    review_id,
    hotel_id,
    customer_id,
    review_date,
    rating,
    sentiment,
    category,
    source_system
FROM {{ source('silver_hospitality', 'guest_review_clean') }}
