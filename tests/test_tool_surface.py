"""The tool boundary is the safety boundary.

The model may only reach the marts through this fixed surface, so the surface itself
is a contract: every advertised tool is dispatchable, every dispatchable tool is
advertised, and the specs stay valid OpenAI function schemas (the Foundry twin
regenerates its OpenAPI operations from them).
"""
from __future__ import annotations

import pytest

from ai.app.agent import ALL_TOOL_SPECS, DISPATCH
from ai.tools.business_tools import TOOL_DISPATCH, TOOL_SPECS
from ai.rag.retriever import RAG_TOOL_SPEC

EXPECTED_TOOLS = {
    "get_underperforming_properties",
    "get_hotel_revenue_risk",
    "get_venue_conversion_risk",
    "get_investment_risk_exposure",
    "get_declining_customer_segments",
    "get_top_business_actions",
    "get_pipeline_status",
    "get_data_quality_trust_score",
    "search_policy_docs",
}


def _names(specs):
    return {s["function"]["name"] for s in specs}


class TestSurfaceContract:
    def test_the_exposed_tool_set_is_exactly_the_approved_set(self):
        """A new tool must be a deliberate change here, never an accident."""
        assert _names(ALL_TOOL_SPECS) == EXPECTED_TOOLS

    def test_every_advertised_tool_is_dispatchable(self):
        assert _names(ALL_TOOL_SPECS) == set(DISPATCH)

    def test_no_undeclared_tool_is_reachable(self):
        """A callable in the dispatch table with no spec would be invisible to review."""
        assert set(DISPATCH) - _names(ALL_TOOL_SPECS) == set()

    def test_rag_tool_is_the_only_non_mart_tool(self):
        assert _names(TOOL_SPECS) == set(TOOL_DISPATCH)
        assert RAG_TOOL_SPEC["function"]["name"] == "search_policy_docs"
        assert RAG_TOOL_SPEC["function"]["name"] not in TOOL_DISPATCH

    def test_every_tool_is_callable(self):
        for name, fn in DISPATCH.items():
            assert callable(fn), f"{name} is not callable"


class TestSpecValidity:
    @pytest.mark.parametrize("spec", ALL_TOOL_SPECS, ids=lambda s: s["function"]["name"])
    def test_spec_is_a_wellformed_function_schema(self, spec):
        assert spec["type"] == "function"
        fn = spec["function"]
        assert fn["name"] and fn["description"]
        params = fn["parameters"]
        assert params["type"] == "object"
        assert isinstance(params.get("properties", {}), dict)

    @pytest.mark.parametrize("spec", ALL_TOOL_SPECS, ids=lambda s: s["function"]["name"])
    def test_description_tells_the_model_when_to_call_it(self, spec):
        """Tool selection accuracy depends on these; an empty one silently degrades routing."""
        assert len(spec["function"]["description"].strip()) > 20


class TestTrustGateIsReachable:
    def test_the_trust_tools_exist(self):
        """The agent must be able to check trust before recommending."""
        assert "get_data_quality_trust_score" in DISPATCH
        assert "get_pipeline_status" in DISPATCH
