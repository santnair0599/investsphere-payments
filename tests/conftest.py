"""Shared fixtures.

Every Azure-native feature is env-flagged, so a stray flag in the developer's shell
would silently change what these tests exercise. Scrub them before each test and let
individual tests opt back in via monkeypatch.
"""
from __future__ import annotations

import pytest

FEATURE_FLAGS = [
    "AGENT_RUNTIME",
    "MODEL_ROUTER_ENABLED",
    "EVAL_GATE_ENABLED",
    "TRACING_ENABLED",
    "PROMPT_SHIELDS_ENABLED",
    "LIVE_GROUNDEDNESS_ENABLED",
    "LIVE_GROUNDEDNESS_SAMPLE_RATE",
    "SENTIMENT_ENRICHMENT_ENABLED",
    "MCP_ENABLE_ACTIONS",
    "AGENTIC_RETRIEVAL_ENABLED",
    "DOC_INTEL_ENABLED",
    "TEAMS_ENABLED",
]

AZURE_ENDPOINTS = [
    "AZURE_CONTENT_SAFETY_ENDPOINT",
    "AZURE_CONTENT_SAFETY_KEY",
    "AZURE_OPENAI_DEPLOYMENT",
    "AZURE_OPENAI_DEPLOYMENT_FAST",
    "AZURE_OPENAI_DEPLOYMENT_REASONING",
]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in FEATURE_FLAGS + AZURE_ENDPOINTS:
        monkeypatch.delenv(name, raising=False)
