-- Customer sentiment fact. Grain: segment x review_month. Cross-domain read of
-- hospitality guest reviews, attributed to a CRM segment via
-- guest_review_clean.customer_id -> contact_clean -> account_clean.
{{ config(materialized='table') }}

WITH reviews AS (
    SELECT
        customer_id,
        CAST(rating AS DOUBLE)         AS rating,
        upper(trim(sentiment))         AS sentiment,
        date_trunc('month', review_date) AS review_month
    FROM {{ source('silver_hospitality', 'guest_review_clean') }}
    WHERE customer_id IS NOT NULL
      AND review_date IS NOT NULL
),

-- resolve each customer_id to a single segment (active contact wins) to avoid
-- fan-out when a customer maps to multiple contacts/accounts
cust_seg AS (
    SELECT customer_id, segment
    FROM (
        SELECT
            c.customer_id,
            a.segment,
            ROW_NUMBER() OVER (
                PARTITION BY c.customer_id
                ORDER BY c.is_active DESC, c.contact_id
            ) AS rn
        FROM {{ source('silver_customer', 'contact_clean') }} c
        JOIN {{ source('silver_customer', 'account_clean') }} a
            ON c.account_id = a.account_id
        WHERE c.customer_id IS NOT NULL
    ) t
    WHERE rn = 1
),

attributed AS (
    SELECT
        cs.segment,
        r.review_month,
        r.rating,
        r.sentiment,
        CASE r.sentiment
            WHEN 'POSITIVE' THEN 1
            WHEN 'NEUTRAL'  THEN 0
            WHEN 'NEGATIVE' THEN -1
        END AS sentiment_score
    FROM reviews r
    JOIN cust_seg cs ON r.customer_id = cs.customer_id
)

SELECT
    segment,
    review_month,
    COUNT(*)                                               AS review_count,
    AVG(rating)                                            AS avg_rating,
    100.0 * COUNT(CASE WHEN sentiment = 'NEGATIVE' THEN 1 END)
        / NULLIF(COUNT(*), 0)                             AS negative_review_pct,
    AVG(sentiment_score)                                  AS avg_sentiment_score
FROM attributed
GROUP BY segment, review_month
