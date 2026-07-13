-- Customer activity fact. Grain: segment x activity_month (derived from
-- opportunity close_date). Blends the CRM deal pipeline (opportunity_clean) with
-- contact activity (contact_clean), attributed to a segment via account_clean.
{{ config(materialized='table') }}

WITH opp AS (
    SELECT
        o.opportunity_id,
        o.account_id,
        a.segment,
        date_trunc('month', o.close_date) AS activity_month,
        o.is_won,
        o.amount,
        o.close_date
    FROM {{ source('silver_customer', 'opportunity_clean') }} o
    JOIN {{ source('silver_customer', 'account_clean') }} a
        ON o.account_id = a.account_id
    WHERE o.close_date IS NOT NULL
),

-- won-deal metrics per segment/month
monthly_opp AS (
    SELECT
        segment,
        activity_month,
        COUNT(CASE WHEN is_won THEN opportunity_id END)      AS opportunities_won,
        COALESCE(SUM(CASE WHEN is_won THEN amount END), 0)   AS won_revenue
    FROM opp
    GROUP BY segment, activity_month
),

-- accounts with pipeline activity in the month
active_accts AS (
    SELECT DISTINCT segment, activity_month, account_id
    FROM opp
),

-- distinct ACTIVE contacts at those active accounts -> "active customers"
active_cust AS (
    SELECT
        aa.segment,
        aa.activity_month,
        COUNT(DISTINCT c.contact_id) AS active_customers
    FROM active_accts aa
    JOIN {{ source('silver_customer', 'contact_clean') }} c
        ON aa.account_id = c.account_id AND c.is_active = true
    GROUP BY aa.segment, aa.activity_month
),

-- first month an account appears in the pipeline -> "new customers" that month
first_seen AS (
    SELECT
        account_id,
        segment,
        date_trunc('month', MIN(close_date)) AS first_month
    FROM opp
    GROUP BY account_id, segment
),

new_cust AS (
    SELECT segment, first_month AS activity_month, COUNT(*) AS new_customers
    FROM first_seen
    GROUP BY segment, first_month
)

SELECT
    m.segment,
    m.activity_month,
    COALESCE(ac.active_customers, 0) AS active_customers,
    COALESCE(nc.new_customers, 0)    AS new_customers,
    m.opportunities_won,
    m.won_revenue
FROM monthly_opp m
LEFT JOIN active_cust ac
    ON m.segment = ac.segment AND m.activity_month = ac.activity_month
LEFT JOIN new_cust nc
    ON m.segment = nc.segment AND m.activity_month = nc.activity_month
