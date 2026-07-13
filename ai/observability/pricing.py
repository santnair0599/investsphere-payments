"""
Azure OpenAI token pricing → per-run cost estimate.

Rates are USD per 1,000,000 tokens (approximate list prices — update to your region /
negotiated / PTU-amortised rates). `estimate_cost` turns a run's token usage into the
`estimated_cost` written to `ai_control.agent_runs`, so "what is this costing me?" becomes
a SQL query, not a guess. Pure Python — no deps, safe to import anywhere.
"""
from __future__ import annotations

# USD per 1M tokens: (input, output). Override by editing here or per-deployment.
PRICING = {
    "gpt-4o":                 {"input": 2.50, "output": 10.00},
    "gpt-4o-mini":            {"input": 0.15, "output": 0.60},
    "gpt-4.1":                {"input": 2.00, "output": 8.00},
    "gpt-4.1-mini":           {"input": 0.40, "output": 1.60},
    "o4-mini":                {"input": 1.10, "output": 4.40},
    "text-embedding-3-small": {"input": 0.02, "output": 0.00},
    "text-embedding-3-large": {"input": 0.13, "output": 0.00},
}
DEFAULT = {"input": 2.50, "output": 10.00}  # fall back to a GPT-4o-class rate


def price_for(model: str | None) -> dict:
    """Match a deployment name to a rate (deployment names may carry suffixes).

    Longest name first, so ``gpt-4o-mini`` matches its own rate rather than the
    ``gpt-4o`` prefix.
    """
    key = (model or "").lower()
    for name in sorted(PRICING, key=len, reverse=True):
        if key.startswith(name):
            return PRICING[name]
    return DEFAULT


def estimate_cost(model: str | None, tokens_in: int | None, tokens_out: int | None) -> float:
    """USD cost for one call/run given input/output token counts."""
    rate = price_for(model)
    return round((tokens_in or 0) / 1e6 * rate["input"]
                 + (tokens_out or 0) / 1e6 * rate["output"], 6)
