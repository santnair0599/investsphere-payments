-- Per-source freshness: how long since each source last loaded, vs a 26-hour
-- SLA (daily feeds + buffer). Answers: "Is source data within SLA?"
{{ config(materialized='table') }}

WITH latest_load AS (
    SELECT
        source_system,
        source_table,
        watermark_value,
        end_time,
        status,
        ROW_NUMBER() OVER (
            PARTITION BY source_system, source_table
            ORDER BY end_time DESC
        ) AS rn
    FROM {{ source('silver_control', 'table_load_status') }}
)

SELECT
    source_system,
    source_table,
    watermark_value,
    end_time                                              AS last_load_time,
    CAST(
        (unix_timestamp(current_timestamp()) - unix_timestamp(end_time)) / 3600.0
        AS DECIMAL(9,2)
    )                                                     AS hours_since_load,
    26.0                                                  AS sla_hours,
    (
        (unix_timestamp(current_timestamp()) - unix_timestamp(end_time)) / 3600.0
        <= 26.0
    )                                                     AS within_sla,
    status
FROM latest_load
WHERE rn = 1
