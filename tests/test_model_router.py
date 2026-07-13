"""Model router: cheap lookups on the fast deployment, synthesis on the reasoning one.

The router is a pure function over env, so it is fully testable with no SDK.
"""
from __future__ import annotations

import pytest

from ai.app.model_router import enabled, is_simple, route


@pytest.fixture
def routed(monkeypatch):
    monkeypatch.setenv("MODEL_ROUTER_ENABLED", "true")
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT_FAST", "gpt-4o-mini")
    monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT_REASONING", "gpt-4o")


class TestDefaultsAreSafe:
    def test_router_is_off_by_default(self):
        assert enabled() is False

    def test_disabled_router_always_uses_the_single_deployment(self, monkeypatch):
        monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT_FAST", "gpt-4o-mini")
        deployment, reason = route("Which hotels have revenue risk?")
        assert deployment == "gpt-4o"
        assert reason == "router_disabled"

    def test_enabled_but_no_fast_deployment_falls_back(self, monkeypatch):
        monkeypatch.setenv("MODEL_ROUTER_ENABLED", "true")
        monkeypatch.setenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        deployment, reason = route("what is the RevPAR definition?")
        assert deployment == "gpt-4o"
        assert reason == "fast_unset_fallback"


class TestIntentClassification:
    @pytest.mark.parametrize("q", [
        "Why did the pipeline go PARTIAL?",
        "Can we trust today's data quality?",
        "What is the RevPAR definition according to policy?",
        "Which sources are stale?",
    ])
    def test_lookup_and_ops_questions_are_simple(self, q):
        assert is_simple(q) is True

    @pytest.mark.parametrize("q", [
        "What are the top business actions this week?",
        "Which properties are underperforming and what should we do?",
        "Summarise investment risk across all business units for leadership",
    ])
    def test_synthesis_questions_are_not_simple(self, q):
        assert is_simple(q) is False

    def test_complex_intent_wins_over_a_simple_keyword(self):
        """A question containing both must not be downgraded to the cheap model."""
        assert is_simple("Given the DQ trust score, what should we prioritise?") is False


class TestRouting:
    def test_simple_lookup_goes_to_fast(self, routed):
        deployment, reason = route("Why did the pipeline go PARTIAL?")
        assert deployment == "gpt-4o-mini"
        assert reason == "simple_lookup->fast"

    def test_complex_synthesis_goes_to_reasoning(self, routed):
        deployment, reason = route("What are the top business actions this week?",
                                   turn_kind="synthesis")
        assert deployment == "gpt-4o"
        assert reason == "complex_synthesis->reasoning"

    def test_tool_selection_turns_stay_cheap_even_when_complex(self, routed):
        """Gathering evidence is a cheap turn; only the final synthesis needs the big model."""
        deployment, reason = route("What are the top business actions this week?",
                                   turn_kind="tool_selection")
        assert deployment == "gpt-4o-mini"
        assert reason == "complex_tool_selection->fast"
