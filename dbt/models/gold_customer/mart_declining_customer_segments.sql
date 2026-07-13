-- Declining-segment answer surface for the AI agent's
-- get_declining_customer_segments(period) tool. One row per segment comparing
-- the latest month vs the prior month across activity + sentiment, plus a
-- segment-level churn rate, with a boolean decline flag and the reasons that fired.
{{ config(materialized='table') }}

WITH act AS (
    SELECT
        segment,
        activity_month,
        active_customers,
        won_revenue,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY activity_month DESC) AS rn
    FROM {{ ref('fact_customer_activity') }}
),

act_cmp AS (
    SELECT
        segment,
        MAX(CASE WHEN rn = 1 THEN activity_month END)    AS as_of_period,
        MAX(CASE WHEN rn = 1 THEN active_customers END)  AS active_customers,
        MAX(CASE WHEN rn = 2 THEN active_customers END)  AS prior_active_customers,
        MAX(CASE WHEN rn = 1 THEN won_revenue END)       AS won_revenue,
        MAX(CASE WHEN rn = 2 THEN won_revenue END)       AS prior_won_revenue
    FROM act
    WHERE rn <= 2
    GROUP BY segment
),

sent AS (
    SELECT
        segment,
        review_month,
        avg_sentiment_score,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY review_month DESC) AS rn
    FROM {{ ref('fact_customer_sentiment') }}
),

sent_cmp AS (
    SELECT
        segment,
        MAX(CASE WHEN rn = 1 THEN avg_sentiment_score END) AS avg_sentiment_score,
        MAX(CASE WHEN rn = 2 THEN avg_sentiment_score END) AS prior_sentiment_score
    FROM sent
    WHERE rn <= 2
    GROUP BY segment
),

-- segment-level churn: inactive contacts / total contacts
churn AS (
    SELECT
        a.segment,
        COUNT(CASE WHEN c.is_active = false THEN 1 END) * 1.0
            / NULLIF(COUNT(*), 0) AS churn_rate
    FROM {{ source('silver_customer', 'contact_clean') }} c
    JOIN {{ source('silver_customer', 'account_clean') }} a
        ON c.account_id = a.account_id
    GROUP BY a.segment
),

metrics AS (
    SELECT
        d.segment,
        COALESCE(ac.active_customers, 0)                        AS active_customers,
        100.0 * (ac.active_customers - ac.prior_active_customers)
            / NULLIF(ac.prior_active_customers, 0)             AS active_change_pct,
        COALESCE(ac.won_revenue, 0)                            AS won_revenue,
        100.0 * (ac.won_revenue - ac.prior_won_revenue)
            / NULLIF(ac.prior_won_revenue, 0)                 AS revenue_change_pct,
        sc.avg_sentiment_score,
        (sc.avg_sentiment_score - sc.prior_sentiment_score)   AS sentiment_change,
        COALESCE(ch.churn_rate, 0)                            AS churn_rate,
        ac.as_of_period
    FROM {{ ref('dim_customer_segment') }} d
    LEFT JOIN act_cmp  ac ON d.segment = ac.segment
    LEFT JOIN sent_cmp sc ON d.segment = sc.segment
    LEFT JOIN churn    ch ON d.segment = ch.segment
)

SELECT
    segment,
    active_customers,
    active_change_pct,
    won_revenue,
    revenue_change_pct,
    avg_sentiment_score,
    sentiment_change,
    churn_rate,
    (
        COALESCE(active_change_pct < -5, false)
        OR COALESCE(revenue_change_pct < -8, false)
        OR COALESCE(sentiment_change < 0 AND avg_sentiment_score < 0, false)
    ) AS is_declining,
    concat_ws('; ',
        CASE WHEN active_change_pct < -5
             THEN concat('active customers down ', round(active_change_pct, 1), '%') END,
        CASE WHEN revenue_change_pct < -8
             THEN concat('won revenue down ', round(revenue_change_pct, 1), '%') END,
        CASE WHEN sentiment_change < 0 AND avg_sentiment_score < 0
             THEN concat('sentiment declining to ', round(avg_sentiment_score, 2)) END
    ) AS risk_reasons,
    as_of_period
FROM metrics
