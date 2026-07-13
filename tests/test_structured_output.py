"""The recommendation contract.

/recommend is consumed by APIs and by the Teams publisher, so BusinessRecommendation
must reject malformed payloads rather than pass prose through.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from ai.app.schemas import RECOMMENDATION_JSON_SCHEMA, BusinessRecommendation, RiskItem
from ai.ci.checks import authorization_pass_rate, sql_is_authorized, structured_output_validity

VALID = {
    "summary": "2 properties underperforming; review pricing before renewal.",
    "items": [{
        "domain": "real_estate",
        "entity_id": "P-1",
        "entity_name": "Downtown Tower",
        "metrics": {"occupancy": 0.62},
        "risk_reasons": "occupancy < 70% for 3 months",
        "recommended_action": "reprice at renewal (advisory)",
    }],
    "confidence": "MEDIUM",
    "trust_reasons": "DQ gate passed; 1 source stale",
    "citations": ["gold_realestate.mart_property_underperformance"],
}


class TestRecommendationSchema:
    def test_a_valid_recommendation_validates(self):
        rec = BusinessRecommendation.model_validate(VALID)
        assert rec.confidence == "MEDIUM"
        assert rec.items[0].entity_id == "P-1"
        assert rec.citations

    def test_summary_is_required(self):
        payload = {k: v for k, v in VALID.items() if k != "summary"}
        with pytest.raises(ValidationError):
            BusinessRecommendation.model_validate(payload)

    def test_confidence_is_required(self):
        """An answer without a confidence level is exactly what the trust gate exists to prevent."""
        payload = {k: v for k, v in VALID.items() if k != "confidence"}
        with pytest.raises(ValidationError):
            BusinessRecommendation.model_validate(payload)

    def test_a_risk_item_must_carry_a_recommended_action(self):
        with pytest.raises(ValidationError):
            RiskItem.model_validate({"domain": "hospitality", "entity_id": "H-3"})

    def test_an_empty_recommendation_is_still_structured(self):
        """No qualifying rows is a valid grounded answer — it must not be a free-text escape."""
        rec = BusinessRecommendation.model_validate(
            {"summary": "No qualifying records for the current filters.",
             "confidence": "HIGH", "items": [], "citations": []})
        assert rec.items == []

    def test_prose_is_rejected(self):
        with pytest.raises(ValidationError):
            BusinessRecommendation.model_validate("Hotel H-3 looks risky.")

    def test_json_schema_is_strict_for_azure_structured_outputs(self):
        assert RECOMMENDATION_JSON_SCHEMA["strict"] is True
        assert RECOMMENDATION_JSON_SCHEMA["name"] == "business_recommendation"
        assert "properties" in RECOMMENDATION_JSON_SCHEMA["schema"]


class TestQualityGateChecks:
    def test_structured_output_validity_is_total(self):
        assert structured_output_validity() == 1.0

    def test_authorization_cases_all_pass(self):
        assert authorization_pass_rate() == 1.0


class TestSqlAuthorization:
    @pytest.mark.parametrize("sql", [
        "SELECT * FROM gold_hospitality.mart_hotel_revenue_risk",
        "SELECT entity_id, risk_reasons FROM gold_investment.mart_investment_risk",
    ])
    def test_read_only_select_is_authorized(self, sql):
        assert sql_is_authorized(sql) is True

    @pytest.mark.parametrize("sql", [
        "DROP TABLE gold.fact_payments",
        "UPDATE gold.dim_customer SET status='X'",
        "DELETE FROM silver_clean.payment_clean",
        "GRANT SELECT ON gold.fact_payments TO analyst",
        "MERGE INTO gold.dim_customer USING x ON 1=1",
    ])
    def test_writes_are_refused(self, sql):
        assert sql_is_authorized(sql) is False

    def test_stacked_statements_are_refused(self):
        assert sql_is_authorized("SELECT 1; DELETE FROM gold.fact_payments") is False

    @pytest.mark.parametrize("sql", [
        "SELECT email FROM silver_cdc.customer_scd2",
        "SELECT iban, phone FROM gold_customer.dim_customer_segment",
    ])
    def test_raw_pii_columns_are_refused(self, sql):
        """Raw PII must be reached through masked serving views, never a tool query."""
        assert sql_is_authorized(sql) is False
