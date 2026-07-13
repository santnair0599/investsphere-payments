-- Business-question surface: "Which hotels have revenue risk?"
-- Compares the latest 30-day window against the prior 30-day window per hotel
-- (RevPAR, occupancy) and blends in guest-review signal (avg rating, negative %).
-- The agent's get_hotel_revenue_risk(period) tool reads this mart.
{{ config(materialized='table') }}

-- Anchor windows on the latest data date (per fact) instead of the cluster
-- clock, so the mart populates regardless of when it runs on synthetic data.
WITH ref_rev AS (
    SELECT MAX(revenue_date) AS as_of FROM {{ ref('fact_revenue') }}
),

ref_rev_review AS (
    SELECT MAX(review_date) AS as_of FROM {{ ref('fact_guest_review') }}
),

revenue AS (
    SELECT
        hotel_id,
        revenue_date,
        revpar,
        occupancy_rate
    FROM {{ ref('fact_revenue') }}
    WHERE revenue_date > DATE_SUB((SELECT as_of FROM ref_rev), 60)
),

-- latest 30 days vs the 30 days before that, aggregated per hotel
revenue_windows AS (
    SELECT
        hotel_id,
        AVG(CASE WHEN revenue_date > DATE_SUB((SELECT as_of FROM ref_rev), 30)
                 THEN revpar END)          AS current_revpar,
        AVG(CASE WHEN revenue_date <= DATE_SUB((SELECT as_of FROM ref_rev), 30)
                 THEN revpar END)          AS prior_revpar,
        AVG(CASE WHEN revenue_date > DATE_SUB((SELECT as_of FROM ref_rev), 30)
                 THEN occupancy_rate END)  AS current_occupancy,
        AVG(CASE WHEN revenue_date <= DATE_SUB((SELECT as_of FROM ref_rev), 30)
                 THEN occupancy_rate END)  AS prior_occupancy
    FROM revenue
    GROUP BY hotel_id
),

reviews AS (
    SELECT
        hotel_id,
        AVG(CAST(rating AS DOUBLE))                                        AS avg_rating,
        AVG(CASE WHEN sentiment = 'NEGATIVE' THEN 1.0 ELSE 0.0 END) * 100  AS negative_review_pct
    FROM {{ ref('fact_guest_review') }}
    WHERE review_date > DATE_SUB((SELECT as_of FROM ref_rev_review), 30)
    GROUP BY hotel_id
),

metrics AS (
    SELECT
        h.hotel_id,
        h.hotel_name,
        h.city,
        h.emirate,
        ROUND(rw.current_revpar, 2)                                        AS revpar,
        -- guard divide-by-zero on the prior window
        ROUND(
            (rw.current_revpar - rw.prior_revpar)
            / NULLIF(rw.prior_revpar, 0) * 100, 2)                         AS revpar_change_pct,
        ROUND(rw.current_occupancy, 4)                                     AS occupancy_rate,
        ROUND(rw.current_occupancy - rw.prior_occupancy, 4)               AS occupancy_change_ppts,
        ROUND(rv.avg_rating, 2)                                            AS avg_rating,
        ROUND(COALESCE(rv.negative_review_pct, 0), 2)                      AS negative_review_pct
    FROM {{ ref('dim_hotel') }} h
    LEFT JOIN revenue_windows rw ON h.hotel_id = rw.hotel_id
    LEFT JOIN reviews         rv ON h.hotel_id = rv.hotel_id
)

SELECT
    hotel_id,
    hotel_name,
    city,
    emirate,
    revpar,
    revpar_change_pct,
    occupancy_rate,
    occupancy_change_ppts,
    avg_rating,
    negative_review_pct,
    -- risk if RevPAR down >8%, occupancy down >0.05, or avg rating below 3.5
    COALESCE(revpar_change_pct < -8, FALSE)
        OR COALESCE(occupancy_change_ppts < -0.05, FALSE)
        OR COALESCE(avg_rating < 3.5, FALSE)                              AS is_revenue_risk,
    CONCAT_WS('; ',
        CASE WHEN revpar_change_pct < -8
             THEN CONCAT('RevPAR down ', ABS(revpar_change_pct), '%') END,
        CASE WHEN occupancy_change_ppts < -0.05
             THEN CONCAT('Occupancy down ', ABS(occupancy_change_ppts), ' ppts') END,
        CASE WHEN avg_rating < 3.5
             THEN CONCAT('Avg rating ', avg_rating, ' below 3.5') END
    )                                                                     AS risk_reasons,
    (SELECT as_of FROM ref_rev)                                           AS as_of_date
FROM metrics
