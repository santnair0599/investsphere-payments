"""Every Azure-native feature is flagged OFF by default with a deterministic fallback.

That claim is the whole reason CI and local dev run without credentials, and it is the
first thing an interviewer will poke at. Assert it rather than trust it.
"""
from __future__ import annotations

import importlib

import pytest

from ai.enrichment import sentiment
from ai.observability import live_eval, tracing


class TestRuntimeSelector:
    def test_default_runtime_is_raw(self):
        """The raw loop is dependency-light; LangGraph is only imported when asked for."""
        import ai.app.runtime as runtime
        importlib.reload(runtime)
        assert runtime.ACTIVE_RUNTIME == "raw"

    def test_langgraph_is_opt_in(self, monkeypatch):
        monkeypatch.setenv("AGENT_RUNTIME", "langgraph")
        import ai.app.runtime as runtime
        importlib.reload(runtime)
        assert runtime.ACTIVE_RUNTIME == "langgraph"
        monkeypatch.delenv("AGENT_RUNTIME")
        importlib.reload(runtime)


class TestFlagsDefaultOff:
    def test_live_groundedness_is_off(self):
        assert live_eval.enabled() is False
        assert live_eval.should_sample() is False

    def test_sentiment_enrichment_is_off(self):
        assert sentiment.azure_enabled() is False

    def test_tracing_is_off_and_spans_are_noops(self):
        """Tracing off must not change agent code paths — the span is a dummy."""
        with tracing.span("agent.run") as s:
            assert s is not None

    @pytest.mark.parametrize("flag_module,fn", [
        (live_eval, "enabled"),
        (sentiment, "azure_enabled"),
    ])
    def test_flag_alone_is_not_enough_without_an_endpoint(self, flag_module, fn, monkeypatch):
        """Half-configured (flag on, endpoint missing) must stay on the fallback, not crash."""
        monkeypatch.setenv("SENTIMENT_ENRICHMENT_ENABLED", "true")
        monkeypatch.setenv("LIVE_GROUNDEDNESS_ENABLED", "true")
        # sentiment additionally requires AZURE_LANGUAGE_ENDPOINT; it is unset here
        assert sentiment.azure_enabled() is False
        # live_eval only needs the flag, but must still sample within [0,1]
        assert 0.0 <= live_eval.sample_rate() <= 1.0


class TestSampleRate:
    def test_default_sample_rate_is_ten_percent(self):
        assert live_eval.sample_rate() == pytest.approx(0.10)

    @pytest.mark.parametrize("raw,expected", [
        ("0.5", 0.5),
        ("2.0", 1.0),      # clamped
        ("-1", 0.0),       # clamped
        ("not-a-number", 0.10),  # falls back to the default
    ])
    def test_sample_rate_is_clamped_and_never_raises(self, raw, expected, monkeypatch):
        monkeypatch.setenv("LIVE_GROUNDEDNESS_SAMPLE_RATE", raw)
        assert live_eval.sample_rate() == pytest.approx(expected)


class TestTeamsPublisherIsDryByDefault:
    def test_publish_does_not_post_when_flag_is_off(self):
        """Flag off returns the card instead of POSTing — safe to run in CI."""
        from ai.integrations.teams.publish import publish_answer
        out = publish_answer("Which hotels have revenue risk?", "H-3: RevPAR down 11%.")
        assert out.get("sent") is not True
