-- Latest pipeline-run outcome per job per run_date, with duration and any
-- PARTIAL/FAILED reason. Answers: "What is today's pipeline status? Why PARTIAL?"
{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        run_id,
        run_date,
        environment,
        job_name,
        status,
        start_time,
        end_time,
        triggered_by,
        error_message,
        ROW_NUMBER() OVER (
            PARTITION BY run_date, job_name
            ORDER BY start_time DESC
        ) AS rn
    FROM {{ source('silver_control', 'pipeline_run') }}
)

SELECT
    run_id,
    run_date,
    environment,
    job_name,
    status,                                               -- STARTED/SUCCESS/FAILED/PARTIAL
    start_time,
    end_time,
    CAST(
        (unix_timestamp(end_time) - unix_timestamp(start_time)) AS BIGINT
    )                                                     AS duration_seconds,
    triggered_by,
    error_message,
    (status = 'PARTIAL')                                  AS is_partial,
    (status = 'FAILED')                                   AS is_failed
FROM ranked
WHERE rn = 1
