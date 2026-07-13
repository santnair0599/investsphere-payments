"""
Structured output schemas for risk + recommendation responses.

Tool results are already structured JSON (each mart tool returns typed rows). This adds
a schema-constrained **recommendation** so the agent's leadership answer is also
structured JSON — not just prose — via Azure OpenAI structured outputs.
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class RiskItem(BaseModel):
    """One asset/segment flagged for attention, with its drivers + advised action."""
    domain: str = Field(description="real_estate/hospitality/entertainment/investment/customer")
    entity_id: str = Field(description="e.g. property_id / hotel_id / venue_id / asset_id / segment")
    entity_name: str | None = None
    metrics: dict = Field(default_factory=dict, description="the figures that fired (from the mart)")
    risk_reasons: str | None = Field(default=None, description="cited drivers from the mart")
    recommended_action: str = Field(description="advisory action; NOT auto-executed")


class BusinessRecommendation(BaseModel):
    """The agent's structured leadership recommendation (schema-constrained JSON)."""
    summary: str = Field(description="one-line headline answer")
    items: list[RiskItem] = Field(default_factory=list)
    confidence: str = Field(description="HIGH / MEDIUM / LOW — from the data trust score")
    trust_reasons: str | None = Field(default=None, description="why that confidence")
    citations: list[str] = Field(default_factory=list, description="policy docs / marts cited")


# JSON Schema form (for response_format={"type":"json_schema"} / tool contracts / OpenAPI).
RECOMMENDATION_JSON_SCHEMA = {
    "name": "business_recommendation",
    "strict": True,
    "schema": BusinessRecommendation.model_json_schema(),
}
