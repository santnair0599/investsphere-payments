-- Booking fact (one row per booking_id) from conformed Silver bookings.
{{ config(materialized='table') }}

SELECT
    booking_id,
    hotel_id,
    customer_id,
    check_in_date,
    check_out_date,
    room_nights,
    adr,
    amount,
    currency_code,
    channel,
    status,
    booking_date,
    source_system
FROM {{ source('silver_hospitality', 'booking_clean') }}
