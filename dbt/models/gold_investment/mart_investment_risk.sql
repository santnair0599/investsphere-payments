-- Investment risk mart: per asset, the latest risk snapshot vs the prior one,
-- flagging assets whose risk exposure is rising. This is the answer surface for
-- the agent tool get_investment_risk_exposure(period).
{{ config(materialized='table') }}

WITH risk_ranked AS (
    SELECT
        asset_id,
        as_of_date,
        var_95,
        volatility,
        concentration_pct,
        risk_rating,
        risk_threshold_breached,
        ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY as_of_date DESC) AS rn
    FROM {{ ref('fact_risk_exposure') }}
),

latest_risk AS (
    SELECT * FROM risk_ranked WHERE rn = 1
),

prior_risk AS (
    SELECT
        asset_id,
        volatility AS prior_volatility
    FROM risk_ranked
    WHERE rn = 2
),

perf_ranked AS (
    SELECT
        asset_id,
        ytd_return,
        benchmark_return,
        ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY as_of_date DESC) AS rn
    FROM {{ ref('fact_asset_performance') }}
),

latest_perf AS (
    SELECT asset_id, ytd_return, benchmark_return
    FROM perf_ranked
    WHERE rn = 1
),

joined AS (
    SELECT
        lr.asset_id,
        d.asset_name,
        d.asset_class,
        d.sector,
        lr.risk_rating,
        lr.volatility,
        -- guard divide-by-zero / missing prior snapshot
        CASE
            WHEN COALESCE(pr.prior_volatility, 0) = 0 THEN 0
            ELSE (lr.volatility - pr.prior_volatility) / pr.prior_volatility * 100
        END AS volatility_change_pct,
        lr.var_95,
        lr.concentration_pct,
        COALESCE(lp.ytd_return - lp.benchmark_return, 0) AS excess_return,
        COALESCE(lr.risk_threshold_breached, FALSE) AS risk_threshold_breached,
        lr.as_of_date
    FROM latest_risk lr
    JOIN {{ ref('dim_asset') }} d
        ON lr.asset_id = d.asset_id
    LEFT JOIN prior_risk pr
        ON lr.asset_id = pr.asset_id
    LEFT JOIN latest_perf lp
        ON lr.asset_id = lp.asset_id
)

SELECT
    asset_id,
    asset_name,
    asset_class,
    sector,
    risk_rating,
    volatility,
    volatility_change_pct,
    var_95,
    concentration_pct,
    excess_return,
    (risk_threshold_breached
        OR risk_rating = 'HIGH'
        OR volatility_change_pct > 20) AS is_rising_risk,
    CONCAT_WS('; ',
        CASE WHEN risk_threshold_breached THEN 'risk threshold breached' END,
        CASE WHEN risk_rating = 'HIGH' THEN 'risk rating HIGH' END,
        CASE WHEN volatility_change_pct > 20
             THEN 'volatility up >20% vs prior snapshot' END
    ) AS risk_reasons,
    as_of_date
FROM joined
