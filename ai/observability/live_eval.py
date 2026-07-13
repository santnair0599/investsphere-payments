"""
Live SAMPLED groundedness monitoring — catches hallucination on production traffic.

Feature-flagged and sampled (NOT every request, for cost/latency). When enabled, a
fraction of live answers are scored for groundedness against the evidence they were
built on (Databricks SQL tool outputs + RAG policy context + citations). Results land in
`ai_control.agent_runs` (groundedness_score, hallucination_flag, evaluation_mode=
'live_sampled') so a SQL rollup gives a **live hallucination rate** — complementing the
deploy-time eval gate.

Safe by default:
  * LIVE_GROUNDEDNESS_ENABLED=false  -> no-op (nothing sampled, no cost/latency)
  * evaluator creds/SDK missing      -> returns None; the user request is NEVER failed
  * evaluator failure is logged separately (not swallowed silently into the answer)

Reuses `ai.eval.azure_evaluators.evaluate_rag` (the Azure AI Evaluation SDK wrapper).
"""
from __future__ import annotations

import logging
import os
import random

log = logging.getLogger("ai.observability.live_eval")

# Groundedness below this is flagged as a likely hallucination (mirrors the eval gate).
HALLUCINATION_THRESHOLD = 0.80


def _truthy(v) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on")


def enabled() -> bool:
    return _truthy(os.environ.get("LIVE_GROUNDEDNESS_ENABLED", ""))


def sample_rate() -> float:
    try:
        return max(0.0, min(1.0, float(os.environ.get("LIVE_GROUNDEDNESS_SAMPLE_RATE", "0.10"))))
    except ValueError:
        return 0.10


def should_sample() -> bool:
    """True only when enabled AND this request falls in the sample (default 10%)."""
    return enabled() and random.random() < sample_rate()


def evaluate_live(question: str, answer: str, context: str) -> dict | None:
    """Score groundedness of `answer` against `context`. Returns
    {groundedness_score, hallucination_flag, evaluation_mode} or None. Never raises."""
    if not (answer and context):
        return None
    try:
        from ai.eval.azure_evaluators import evaluate_rag
        rag = evaluate_rag(question, answer, context)   # {} on failure / unavailable
        score = rag.get("groundedness")
        if score is None:
            log.warning("live_groundedness: no score returned (evaluator creds/SDK unavailable)")
            return None
        score = round(float(score), 3)
        return {"groundedness_score": score,
                "hallucination_flag": score < HALLUCINATION_THRESHOLD,
                "evaluation_mode": "live_sampled"}
    except Exception as exc:  # never break the user request; log separately
        log.warning("live_groundedness: evaluator failed: %s", exc)
        return None
