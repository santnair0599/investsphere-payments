-- Property dimension: one governed row per real-estate asset.
{{ config(materialized='table') }}

SELECT
    property_id,
    property_name,
    property_type,
    city,
    emirate,
    gross_leasable_area_sqm,
    units_total,
    acquisition_date,
    source_system
FROM {{ source('silver_realestate', 'property_clean') }}
