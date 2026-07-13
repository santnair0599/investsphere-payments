-- Customer segment dimension: one row per CRM segment with representative
-- attributes (most-common industry/region) and segment-level account counts.
-- Grain: segment (unique). Source: silver_customer.account_clean.
{{ config(materialized='table') }}

WITH accounts AS (
    SELECT account_id, segment, industry, region
    FROM {{ source('silver_customer', 'account_clean') }}
),

-- most-common industry per segment (tie-break alphabetically for determinism)
industry_rank AS (
    SELECT
        segment,
        industry,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY COUNT(*) DESC, industry) AS rn
    FROM accounts
    WHERE industry IS NOT NULL
    GROUP BY segment, industry
),

-- most-common region per segment
region_rank AS (
    SELECT
        segment,
        region,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY COUNT(*) DESC, region) AS rn
    FROM accounts
    WHERE region IS NOT NULL
    GROUP BY segment, region
),

seg AS (
    SELECT
        segment,
        COUNT(DISTINCT account_id) AS account_count,
        COUNT(DISTINCT industry)   AS industry_count,
        COUNT(DISTINCT region)     AS region_count
    FROM accounts
    GROUP BY segment
)

SELECT
    s.segment,
    ir.industry       AS primary_industry,
    rr.region         AS primary_region,
    s.account_count,
    s.industry_count,
    s.region_count
FROM seg s
LEFT JOIN industry_rank ir ON s.segment = ir.segment AND ir.rn = 1
LEFT JOIN region_rank   rr ON s.segment = rr.segment AND rr.rn = 1
