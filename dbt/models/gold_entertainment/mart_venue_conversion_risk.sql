-- Business question: "Which venues have high/healthy footfall but weak ticket
-- conversion?" Compares the latest 30-day window vs the prior 30-day window per
-- venue. A venue is flagged when footfall is holding up (not declining) yet the
-- conversion rate is low OR falling. All ratios guard divide-by-zero via NULLIF
-- and COALESCE so every metric (and the boolean flag) is non-null.
{{ config(materialized='table') }}

-- Anchor windows on the latest data date (per fact) instead of the cluster
-- clock, so the mart populates regardless of when it runs on synthetic data.
WITH ref_footfall AS (
    SELECT MAX(footfall_date) AS as_of FROM {{ ref('fact_footfall') }}
),

ref_ticket AS (
    SELECT MAX(sale_date) AS as_of FROM {{ ref('fact_ticket_sales') }}
),

ref_campaign AS (
    SELECT MAX(campaign_date) AS as_of FROM {{ ref('fact_campaign_roi') }}
),

footfall_w AS (
    SELECT
        venue_id,
        SUM(CASE WHEN footfall_date >= (SELECT as_of FROM ref_footfall) - INTERVAL 30 DAYS
                 THEN visitors END)                                        AS curr_visitors,
        SUM(CASE WHEN footfall_date >= (SELECT as_of FROM ref_footfall) - INTERVAL 60 DAYS
                  AND footfall_date <  (SELECT as_of FROM ref_footfall) - INTERVAL 30 DAYS
                 THEN visitors END)                                        AS prior_visitors
    FROM {{ ref('fact_footfall') }}
    WHERE footfall_date >= (SELECT as_of FROM ref_footfall) - INTERVAL 60 DAYS
    GROUP BY venue_id
),

ticket_w AS (
    SELECT
        venue_id,
        SUM(CASE WHEN sale_date >= (SELECT as_of FROM ref_ticket) - INTERVAL 30 DAYS
                 THEN quantity END)                                        AS curr_tickets,
        SUM(CASE WHEN sale_date >= (SELECT as_of FROM ref_ticket) - INTERVAL 30 DAYS
                 THEN amount END)                                          AS curr_revenue,
        SUM(CASE WHEN sale_date >= (SELECT as_of FROM ref_ticket) - INTERVAL 60 DAYS
                  AND sale_date <  (SELECT as_of FROM ref_ticket) - INTERVAL 30 DAYS
                 THEN quantity END)                                        AS prior_tickets
    FROM {{ ref('fact_ticket_sales') }}
    WHERE sale_date >= (SELECT as_of FROM ref_ticket) - INTERVAL 60 DAYS
    GROUP BY venue_id
),

campaign_w AS (
    SELECT
        venue_id,
        AVG(roi) AS campaign_roi
    FROM {{ ref('fact_campaign_roi') }}
    WHERE campaign_date >= (SELECT as_of FROM ref_campaign) - INTERVAL 30 DAYS
    GROUP BY venue_id
),

metrics AS (
    SELECT
        v.venue_id,
        v.venue_name,
        v.venue_type,
        v.city,
        COALESCE(f.curr_visitors, 0)                                      AS visitors,
        ROUND(COALESCE(
            (COALESCE(f.curr_visitors, 0) - f.prior_visitors)
                / NULLIF(f.prior_visitors, 0) * 100, 0), 2)               AS footfall_change_pct,
        COALESCE(t.curr_tickets, 0)                                       AS tickets_sold,
        COALESCE(COALESCE(t.curr_tickets, 0)
                 / NULLIF(f.curr_visitors, 0), 0)                         AS conversion_rate,
        COALESCE(COALESCE(t.prior_tickets, 0)
                 / NULLIF(f.prior_visitors, 0), 0)                        AS prior_conversion_rate,
        CAST(COALESCE(t.curr_revenue, 0) AS DECIMAL(18,2))                AS ticket_revenue,
        c.campaign_roi
    FROM {{ ref('dim_venue') }} v
    LEFT JOIN footfall_w f ON v.venue_id = f.venue_id
    LEFT JOIN ticket_w   t ON v.venue_id = t.venue_id
    LEFT JOIN campaign_w c ON v.venue_id = c.venue_id
)

SELECT
    venue_id,
    venue_name,
    venue_type,
    city,
    visitors,
    footfall_change_pct,
    tickets_sold,
    ROUND(conversion_rate, 4)                                             AS conversion_rate,
    ROUND(conversion_rate - prior_conversion_rate, 4)                     AS conversion_change_ppts,
    ticket_revenue,
    ROUND(campaign_roi, 4)                                               AS campaign_roi,
    (
        footfall_change_pct >= 0
        AND (conversion_rate < 0.10
             OR (conversion_rate - prior_conversion_rate) < -0.05)
    )                                                                    AS is_conversion_risk,
    CASE WHEN (
        footfall_change_pct >= 0
        AND (conversion_rate < 0.10
             OR (conversion_rate - prior_conversion_rate) < -0.05)
    ) THEN concat_ws('; ',
        CASE WHEN footfall_change_pct >= 0 THEN 'FOOTFALL_HEALTHY' END,
        CASE WHEN conversion_rate < 0.10 THEN 'LOW_CONVERSION_RATE' END,
        CASE WHEN (conversion_rate - prior_conversion_rate) < -0.05
             THEN 'CONVERSION_DECLINING' END)
    ELSE '' END                                                          AS risk_reasons,
    (SELECT as_of FROM ref_footfall)                                     AS as_of_date
FROM metrics
