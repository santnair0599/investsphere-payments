-- Per-source data-quality gate outcome for the latest run: rows read/written/
-- rejected plus quarantine rate. Answers: "Did the DQ gate pass? How much was
-- quarantined?" Drives the trust score consumed by the AI agent.
{{ config(materialized='table') }}

WITH latest_load AS (
    SELECT
        source_system,
        source_table,
        target_table,
        run_id,
        records_read,
        records_written,
        records_rejected,
        corrupt_record_count,
        status,
        end_time,
        ROW_NUMBER() OVER (
            PARTITION BY source_system, source_table
            ORDER BY end_time DESC
        ) AS rn
    FROM {{ source('silver_control', 'table_load_status') }}
),

quarantine AS (
    SELECT
        source_system,
        source_table,
        run_id,
        COUNT(*) AS quarantined_records
    FROM {{ source('silver_quarantine', 'failed_records') }}
    GROUP BY source_system, source_table, run_id
)

SELECT
    l.source_system,
    l.source_table,
    l.target_table,
    l.run_id,
    l.records_read,
    l.records_written,
    l.records_rejected,
    l.corrupt_record_count,
    COALESCE(q.quarantined_records, 0)                                AS quarantined_records,
    ROUND(
        COALESCE(l.records_rejected, 0) / NULLIF(l.records_read, 0), 4
    )                                                                 AS reject_rate,
    ROUND(
        COALESCE(q.quarantined_records, 0) / NULLIF(l.records_read, 0), 4
    )                                                                 AS quarantine_rate,
    l.status,
    -- gate passes when nothing corrupt and reject rate stays under 5%
    (
        COALESCE(l.corrupt_record_count, 0) = 0
        AND COALESCE(l.records_rejected, 0) / NULLIF(l.records_read, 0) < 0.05
    )                                                                 AS dq_gate_passed,
    l.end_time                                                        AS as_of_time
FROM latest_load l
LEFT JOIN quarantine q
    ON l.source_system = q.source_system
   AND l.source_table = q.source_table
   AND l.run_id = q.run_id
WHERE l.rn = 1
