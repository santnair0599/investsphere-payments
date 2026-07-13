-- Property underperformance mart: the answer surface for
-- get_underperforming_properties(period). Compares the latest 30-day window vs
-- the prior 30-day window per property for occupancy and maintenance cost, plus
-- the active-lease rent roll. is_underperforming + risk_reasons let the AI agent
-- cite grounded, human-readable evidence.
{{ config(materialized='table') }}

-- Anchor windows on the latest data date (per fact) instead of the cluster
-- clock, so the mart populates regardless of when it runs on synthetic data.
WITH ref_occ AS (
    SELECT MAX(occupancy_date) AS as_of FROM {{ ref('fact_occupancy') }}
),

ref_maint AS (
    SELECT MAX(opened_date) AS as_of FROM {{ ref('fact_maintenance') }}
),

occ_current AS (
    SELECT
        property_id,
        AVG(occupancy_rate) AS occupancy_rate
    FROM {{ ref('fact_occupancy') }}
    WHERE occupancy_date >= (SELECT as_of FROM ref_occ) - INTERVAL 30 DAYS
    GROUP BY property_id
),

occ_prior AS (
    SELECT
        property_id,
        AVG(occupancy_rate) AS occupancy_rate
    FROM {{ ref('fact_occupancy') }}
    WHERE occupancy_date >= (SELECT as_of FROM ref_occ) - INTERVAL 60 DAYS
      AND occupancy_date <  (SELECT as_of FROM ref_occ) - INTERVAL 30 DAYS
    GROUP BY property_id
),

maint_current AS (
    SELECT
        property_id,
        SUM(cost) AS maintenance_cost
    FROM {{ ref('fact_maintenance') }}
    WHERE opened_date >= (SELECT as_of FROM ref_maint) - INTERVAL 30 DAYS
    GROUP BY property_id
),

maint_prior AS (
    SELECT
        property_id,
        SUM(cost) AS maintenance_cost
    FROM {{ ref('fact_maintenance') }}
    WHERE opened_date >= (SELECT as_of FROM ref_maint) - INTERVAL 60 DAYS
      AND opened_date <  (SELECT as_of FROM ref_maint) - INTERVAL 30 DAYS
    GROUP BY property_id
),

rent_roll AS (
    SELECT
        property_id,
        SUM(monthly_rent) AS monthly_rent_roll
    FROM {{ ref('fact_lease') }}
    WHERE status = 'ACTIVE'
    GROUP BY property_id
),

metrics AS (
    SELECT
        p.property_id,
        p.property_name,
        p.property_type,
        p.city,
        p.emirate,
        COALESCE(oc.occupancy_rate, 0)            AS occupancy_rate,
        COALESCE(oc.occupancy_rate, 0)
            - COALESCE(op.occupancy_rate, 0)      AS occupancy_change_ppts,
        COALESCE(mc.maintenance_cost, 0)          AS maintenance_cost,
        (COALESCE(mc.maintenance_cost, 0) - COALESCE(mp.maintenance_cost, 0))
            / NULLIF(COALESCE(mp.maintenance_cost, 0), 0) * 100 AS maintenance_cost_change_pct,
        COALESCE(rr.monthly_rent_roll, 0)         AS monthly_rent_roll
    FROM {{ ref('dim_property') }} p
    LEFT JOIN occ_current  oc ON p.property_id = oc.property_id
    LEFT JOIN occ_prior    op ON p.property_id = op.property_id
    LEFT JOIN maint_current mc ON p.property_id = mc.property_id
    LEFT JOIN maint_prior  mp ON p.property_id = mp.property_id
    LEFT JOIN rent_roll    rr ON p.property_id = rr.property_id
)

SELECT
    property_id,
    property_name,
    property_type,
    city,
    emirate,
    occupancy_rate,
    occupancy_change_ppts,
    maintenance_cost,
    COALESCE(maintenance_cost_change_pct, 0) AS maintenance_cost_change_pct,
    monthly_rent_roll,
    (occupancy_change_ppts < -0.05
     OR COALESCE(maintenance_cost_change_pct, 0) > 15) AS is_underperforming,
    CONCAT_WS('; ',
        CASE WHEN occupancy_change_ppts < -0.05
             THEN CONCAT('Occupancy dropped ',
                         ROUND(ABS(occupancy_change_ppts) * 100, 2),
                         ' ppts vs prior 30-day window')
        END,
        CASE WHEN COALESCE(maintenance_cost_change_pct, 0) > 15
             THEN CONCAT('Maintenance cost up ',
                         ROUND(maintenance_cost_change_pct, 1),
                         '% vs prior 30-day window')
        END
    ) AS risk_reasons,
    (SELECT as_of FROM ref_occ) AS as_of_date
FROM metrics
