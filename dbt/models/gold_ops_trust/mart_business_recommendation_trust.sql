-- THE TRUST GATE for the AI agent. Rolls pipeline status + DQ gates + source
-- freshness into a single trust_level + confidence_score + human-readable reasons.
-- The agent calls this ("can we trust today's recommendation?") BEFORE presenting
-- any business recommendation, and cites the reasons back to leadership.
{{ config(materialized='table') }}

WITH pipe AS (
    SELECT
        run_date,
        MAX(CASE WHEN is_failed  THEN 1 ELSE 0 END) AS any_failed,
        MAX(CASE WHEN is_partial THEN 1 ELSE 0 END) AS any_partial,
        COUNT(*)                                    AS jobs
    FROM {{ ref('mart_pipeline_status') }}
    GROUP BY run_date
),

dq AS (
    SELECT
        MIN(CASE WHEN dq_gate_passed THEN 1 ELSE 0 END) AS all_gates_passed,
        MAX(quarantine_rate)                            AS max_quarantine_rate,
        COUNT(*)                                        AS sources_checked
    FROM {{ ref('mart_dq_gate_status') }}
),

fresh AS (
    SELECT
        MIN(CASE WHEN within_sla THEN 1 ELSE 0 END) AS all_within_sla,
        SUM(CASE WHEN within_sla THEN 0 ELSE 1 END) AS stale_sources
    FROM {{ ref('mart_source_freshness') }}
),

-- latest run_date only
latest AS (SELECT MAX(run_date) AS run_date FROM pipe)

SELECT
    l.run_date,
    p.jobs,
    d.sources_checked,
    d.max_quarantine_rate,
    f.stale_sources,
    -- trust level
    CASE
        WHEN p.any_failed = 1 OR d.all_gates_passed = 0 OR d.max_quarantine_rate >= 0.05
            THEN 'LOW'
        WHEN p.any_partial = 1 OR f.all_within_sla = 0 OR d.max_quarantine_rate >= 0.02
            THEN 'MEDIUM'
        ELSE 'HIGH'
    END                                                     AS trust_level,
    -- confidence score 0..1 (start at 1, subtract penalties)
    ROUND(
        1.0
        - (CASE WHEN p.any_failed = 1 THEN 0.5 ELSE 0 END)
        - (CASE WHEN p.any_partial = 1 THEN 0.2 ELSE 0 END)
        - (CASE WHEN d.all_gates_passed = 0 THEN 0.3 ELSE 0 END)
        - (CASE WHEN f.all_within_sla = 0 THEN 0.15 ELSE 0 END)
        - LEAST(COALESCE(d.max_quarantine_rate, 0) * 2, 0.3)
    , 3)                                                    AS confidence_score,
    -- human-readable evidence for the agent to cite
    concat_ws('; ',
        CASE WHEN p.any_failed = 1  THEN 'a pipeline job FAILED' END,
        CASE WHEN p.any_partial = 1 THEN 'a pipeline job completed PARTIAL (a gate blocked part of the flow)' END,
        CASE WHEN d.all_gates_passed = 0 THEN 'one or more DQ gates did not pass' END,
        CASE WHEN d.max_quarantine_rate >= 0.02
             THEN concat('quarantine rate elevated at ',
                         CAST(ROUND(d.max_quarantine_rate * 100, 1) AS STRING), '%') END,
        CASE WHEN f.all_within_sla = 0
             THEN concat(CAST(f.stale_sources AS STRING), ' source(s) outside freshness SLA') END,
        CASE WHEN p.any_failed = 0 AND p.any_partial = 0 AND d.all_gates_passed = 1
                  AND f.all_within_sla = 1 AND COALESCE(d.max_quarantine_rate,0) < 0.02
             THEN 'pipeline SUCCESS, all DQ gates passed, all sources fresh' END
    )                                                       AS trust_reasons,
    current_timestamp()                                     AS evaluated_at
FROM latest l
CROSS JOIN pipe  p
CROSS JOIN dq    d
CROSS JOIN fresh f
WHERE p.run_date = l.run_date
