-- Daily occupancy fact. Incremental with a reprocess window on occupancy_date
-- so late-arriving daily rows are re-merged without a full rebuild.
{{ config(
    materialized='incremental',
    unique_key='occupancy_id',
    incremental_strategy='merge'
) }}

SELECT
    occupancy_id,
    property_id,
    occupancy_date,
    units_occupied,
    units_total,
    occupancy_rate,
    source_system
FROM {{ source('silver_realestate', 'occupancy_clean') }}

{% if is_incremental() %}
-- reprocess the trailing window to absorb late/corrected daily rows
WHERE occupancy_date >= (
    SELECT COALESCE(DATEADD(day, -35, MAX(occupancy_date)), DATE'1900-01-01')
    FROM {{ this }}
)
{% endif %}
