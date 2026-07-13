-- Marketing campaign ROI fact (grain: campaign_id + campaign_date).
-- Attributed revenue is modelled as conversions * the venue's average ticket
-- price, giving an ROI; roas (conversions per click) is kept as a spend-free
-- efficiency measure. All ratios guard divide-by-zero via NULLIF.
{{ config(materialized='table') }}

WITH avg_ticket AS (
    SELECT
        venue_id,
        SUM(amount) / NULLIF(SUM(quantity), 0) AS avg_ticket_price
    FROM {{ ref('fact_ticket_sales') }}
    GROUP BY venue_id
)

SELECT
    c.campaign_id,
    c.venue_id,
    c.channel,
    c.spend,
    c.impressions,
    c.clicks,
    c.conversions,
    c.currency_code,
    c.campaign_date,
    COALESCE(c.conversions * a.avg_ticket_price, 0)                        AS attributed_revenue,
    (COALESCE(c.conversions * a.avg_ticket_price, 0) - c.spend)
        / NULLIF(c.spend, 0)                                              AS roi,
    c.conversions / NULLIF(c.clicks, 0)                                   AS roas,
    c.source_system
FROM {{ source('silver_entertainment', 'campaign_roi_clean') }} c
LEFT JOIN avg_ticket a
    ON c.venue_id = a.venue_id
