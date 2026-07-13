"""
Business tools the Enterprise Decision Agent can call (tool/function calling).

Each tool is a thin, read-only, parameter-validated query over a **governed Gold
mart** (or the ops-trust marts). The agent does NOT guess numbers — it calls these.
Every tool returns structured rows plus the mart it came from, so answers are
traceable. `TOOL_SPECS` is the OpenAI/Azure-AI-Foundry function-schema list.

Guardrails live in `databricks_client` (read-only, bounded rows) and here
(argument validation + a fixed allowlist of marts — the LLM cannot query arbitrary
tables). Write/action tools (e.g. recommend→create action) require human approval
and live behind the MCP adapter, not here.
"""
from __future__ import annotations

from .databricks_client import run_query

# ---- argument validation --------------------------------------------------
VALID_PERIODS = {"this_week", "last_week", "this_month", "last_month", "last_30_days"}
VALID_DOMAINS = {"real_estate", "hospitality", "entertainment", "investment", "customer"}


def _validate_period(period: str) -> str:
    if period not in VALID_PERIODS:
        raise ValueError(f"period must be one of {sorted(VALID_PERIODS)}")
    return period


def _top(rows: list[dict], mart: str, n: int = 10) -> dict:
    return {"mart": mart, "row_count": len(rows), "rows": rows[:n]}


# ---- domain risk tools (SELECT from the flagged mart rows) ----------------
def get_underperforming_properties(period: str = "this_week") -> dict:
    _validate_period(period)
    rows = run_query(
        """
        SELECT property_id, property_name, property_type, city, emirate,
               occupancy_rate, occupancy_change_ppts,
               maintenance_cost, maintenance_cost_change_pct,
               monthly_rent_roll, risk_reasons
        FROM gold_realestate.mart_property_underperformance
        WHERE is_underperforming = true
        ORDER BY occupancy_change_ppts ASC, maintenance_cost_change_pct DESC
        """
    )
    return _top(rows, "gold_realestate.mart_property_underperformance")


def get_hotel_revenue_risk(period: str = "this_week") -> dict:
    _validate_period(period)
    rows = run_query(
        """
        SELECT hotel_id, hotel_name, city, emirate,
               revpar, revpar_change_pct, occupancy_rate, occupancy_change_ppts,
               avg_rating, negative_review_pct, risk_reasons
        FROM gold_hospitality.mart_hotel_revenue_risk
        WHERE is_revenue_risk = true
        ORDER BY revpar_change_pct ASC
        """
    )
    return _top(rows, "gold_hospitality.mart_hotel_revenue_risk")


def get_venue_conversion_risk(period: str = "this_week") -> dict:
    _validate_period(period)
    rows = run_query(
        """
        SELECT venue_id, venue_name, venue_type, city,
               visitors, footfall_change_pct, tickets_sold,
               conversion_rate, conversion_change_ppts, ticket_revenue,
               campaign_roi, risk_reasons
        FROM gold_entertainment.mart_venue_conversion_risk
        WHERE is_conversion_risk = true
        ORDER BY conversion_rate ASC
        """
    )
    return _top(rows, "gold_entertainment.mart_venue_conversion_risk")


def get_investment_risk_exposure(period: str = "this_week") -> dict:
    _validate_period(period)
    rows = run_query(
        """
        SELECT asset_id, asset_name, asset_class, sector,
               risk_rating, volatility, volatility_change_pct,
               var_95, concentration_pct, excess_return, risk_reasons
        FROM gold_investment.mart_investment_risk
        WHERE is_rising_risk = true
        ORDER BY (risk_rating = 'HIGH') DESC, volatility_change_pct DESC
        """
    )
    return _top(rows, "gold_investment.mart_investment_risk")


def get_declining_customer_segments(period: str = "this_month") -> dict:
    _validate_period(period)
    rows = run_query(
        """
        SELECT segment, active_customers, active_change_pct,
               won_revenue, revenue_change_pct,
               avg_sentiment_score, sentiment_change, churn_rate, risk_reasons
        FROM gold_customer.mart_declining_customer_segments
        WHERE is_declining = true
        ORDER BY revenue_change_pct ASC
        """
    )
    return _top(rows, "gold_customer.mart_declining_customer_segments")


# ---- operational trust tools ----------------------------------------------
def get_pipeline_status(run_date: str | None = None) -> dict:
    where = "WHERE run_date = :run_date" if run_date else ""
    rows = run_query(
        f"""
        SELECT run_date, job_name, status, duration_seconds,
               is_partial, is_failed, error_message
        FROM gold_ops_trust.mart_pipeline_status
        {where}
        ORDER BY run_date DESC, job_name
        """,
        params={"run_date": run_date} if run_date else None,
    )
    return _top(rows, "gold_ops_trust.mart_pipeline_status", n=50)


def get_data_quality_trust_score(run_date: str | None = None) -> dict:
    """The trust gate — call BEFORE presenting any recommendation."""
    rows = run_query(
        """
        SELECT run_date, trust_level, confidence_score, max_quarantine_rate,
               stale_sources, trust_reasons, evaluated_at
        FROM gold_ops_trust.mart_business_recommendation_trust
        ORDER BY run_date DESC
        """
    )
    return _top(rows, "gold_ops_trust.mart_business_recommendation_trust", n=1)


# ---- cross-domain aggregator ----------------------------------------------
def get_top_business_actions(period: str = "this_week") -> dict:
    """Union the flagged rows across every domain into one ranked attention list."""
    _validate_period(period)
    domains = [
        get_underperforming_properties(period),
        get_hotel_revenue_risk(period),
        get_venue_conversion_risk(period),
        get_investment_risk_exposure(period),
        get_declining_customer_segments(period),
    ]
    flagged = []
    for d in domains:
        for r in d["rows"]:
            flagged.append({"mart": d["mart"], **r})
    return {"flagged_count": len(flagged), "items": flagged, "period": period}


# ---- OpenAI / Azure AI Foundry function schemas ---------------------------
def _period_param():
    return {
        "period": {
            "type": "string",
            "enum": sorted(VALID_PERIODS),
            "description": "Reporting period window.",
        }
    }


TOOL_SPECS = [
    {"type": "function", "function": {
        "name": "get_underperforming_properties",
        "description": "Real-estate assets with falling occupancy and/or rising maintenance cost.",
        "parameters": {"type": "object", "properties": _period_param()}}},
    {"type": "function", "function": {
        "name": "get_hotel_revenue_risk",
        "description": "Hotels with revenue risk (RevPAR/occupancy decline or poor guest sentiment).",
        "parameters": {"type": "object", "properties": _period_param()}}},
    {"type": "function", "function": {
        "name": "get_venue_conversion_risk",
        "description": "Entertainment venues with high/rising footfall but weak ticket conversion.",
        "parameters": {"type": "object", "properties": _period_param()}}},
    {"type": "function", "function": {
        "name": "get_investment_risk_exposure",
        "description": "Investment assets with rising risk exposure or breached risk thresholds.",
        "parameters": {"type": "object", "properties": _period_param()}}},
    {"type": "function", "function": {
        "name": "get_declining_customer_segments",
        "description": "Customer segments with declining activity, revenue, or sentiment.",
        "parameters": {"type": "object", "properties": _period_param()}}},
    {"type": "function", "function": {
        "name": "get_top_business_actions",
        "description": "Cross-domain ranked list of assets/segments needing leadership attention.",
        "parameters": {"type": "object", "properties": _period_param()}}},
    {"type": "function", "function": {
        "name": "get_pipeline_status",
        "description": "Latest data-pipeline run status; explains why a run was PARTIAL/FAILED.",
        "parameters": {"type": "object", "properties": {
            "run_date": {"type": "string", "description": "YYYY-MM-DD; omit for latest."}}}}},
    {"type": "function", "function": {
        "name": "get_data_quality_trust_score",
        "description": "Trust level + confidence for today's data; call before giving a recommendation.",
        "parameters": {"type": "object", "properties": {
            "run_date": {"type": "string", "description": "YYYY-MM-DD; omit for latest."}}}}},
]

# name -> callable dispatch table used by the agent runtime
TOOL_DISPATCH = {
    "get_underperforming_properties": get_underperforming_properties,
    "get_hotel_revenue_risk": get_hotel_revenue_risk,
    "get_venue_conversion_risk": get_venue_conversion_risk,
    "get_investment_risk_exposure": get_investment_risk_exposure,
    "get_declining_customer_segments": get_declining_customer_segments,
    "get_top_business_actions": get_top_business_actions,
    "get_pipeline_status": get_pipeline_status,
    "get_data_quality_trust_score": get_data_quality_trust_score,
}
