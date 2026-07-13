"""
Azure AI Evaluation SDK integration for the GenAI eval gate.

These wrappers call Microsoft's `azure.ai.evaluation` LLM-judge evaluators
(Groundedness / Relevance / Retrieval / ToolCallAccuracy / TaskAdherence) so the
CI gate scores answers with the same evaluators used in Azure AI Foundry.

Design contract (so the gate degrades gracefully anywhere):
  * The Azure SDK is imported LAZILY inside functions — importing this module must
    succeed with no SDK installed and no credentials.
  * `azure_evals_available()` is the single feature flag. It is True only when the
    gate is explicitly enabled, the SDK imports, and the LLM-judge model config is
    present in the environment.
  * Every public function NEVER raises. On any error, or when unavailable, it
    returns `{}` so `run_evals` transparently falls back to the custom heuristics.
  * SDK scores are on a 1..5 Likert scale; we normalize to 0..1 (divide by 5).
"""
from __future__ import annotations

import os

# Metric name -> the SDK result key(s) that hold the numeric score. The SDK returns
# both a bare key (e.g. "groundedness") and a model-prefixed one (e.g. "gpt_groundedness").
_SCORE_KEYS = {
    "groundedness": ("groundedness", "gpt_groundedness"),
    "relevance": ("relevance", "gpt_relevance"),
    "retrieval": ("retrieval", "gpt_retrieval"),
    "tool_call_accuracy": ("tool_call_accuracy", "tool_call_accurate"),
    "task_adherence": ("task_adherence", "gpt_task_adherence"),
}


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def model_config() -> dict:
    """Build the Azure OpenAI LLM-judge model_config from the environment.

    Returns an empty dict when the mandatory endpoint/deployment are missing.
    """
    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
    deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT")
    if not endpoint or not deployment:
        return {}
    cfg = {
        "azure_endpoint": endpoint,
        "azure_deployment": deployment,
        "api_version": os.environ.get("AZURE_OPENAI_API_VERSION", "2024-10-21"),
    }
    api_key = os.environ.get("AZURE_OPENAI_API_KEY")
    if api_key:
        cfg["api_key"] = api_key
    return cfg


def _sdk_importable() -> bool:
    try:
        import azure.ai.evaluation  # noqa: F401
        return True
    except Exception:
        return False


def azure_evals_available() -> bool:
    """True only when the gate is enabled, the SDK imports, and judge config exists.

    Requires:
      * EVAL_GATE_ENABLED truthy (explicit opt-in — off by default for local/CI dry runs)
      * `azure.ai.evaluation` importable
      * AZURE_OPENAI_ENDPOINT + AZURE_OPENAI_DEPLOYMENT set (the LLM-judge model)
    """
    if not _truthy(os.environ.get("EVAL_GATE_ENABLED")):
        return False
    if not model_config():
        return False
    return _sdk_importable()


def _normalize(raw) -> float | None:
    """SDK Likert score (1..5) -> 0..1. Returns None if not a usable number."""
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    # 1..5 Likert -> 0..1. Values already in 0..1 (bools / rates) pass through.
    if val > 1.0:
        val = val / 5.0
    return max(0.0, min(1.0, val))


def _extract(result: dict, metric: str) -> float | None:
    if not isinstance(result, dict):
        return None
    for key in _SCORE_KEYS.get(metric, (metric,)):
        if key in result:
            score = _normalize(result[key])
            if score is not None:
                return score
    return None


def evaluate_rag(query: str, response: str, context: str,
                 ground_truth: str | None = None) -> dict:
    """RAG-quality metrics for policy / KPI answers, normalized to 0..1.

    Returns a subset of {groundedness, relevance, retrieval}; each metric is
    computed independently and simply omitted if its evaluator fails.
    """
    if not azure_evals_available():
        return {}
    cfg = model_config()
    out: dict = {}
    try:
        from azure.ai.evaluation import (
            GroundednessEvaluator, RelevanceEvaluator, RetrievalEvaluator,
        )
    except Exception:
        return {}

    # Each evaluator is isolated: a failure omits that metric, never the whole dict.
    try:
        r = GroundednessEvaluator(cfg)(query=query, context=context, response=response)
        score = _extract(r, "groundedness")
        if score is not None:
            out["groundedness"] = score
    except Exception:
        pass
    try:
        r = RelevanceEvaluator(cfg)(query=query, response=response)
        score = _extract(r, "relevance")
        if score is not None:
            out["relevance"] = score
    except Exception:
        pass
    try:
        r = RetrievalEvaluator(cfg)(query=query, context=context)
        score = _extract(r, "retrieval")
        if score is not None:
            out["retrieval"] = score
    except Exception:
        pass
    return out


def evaluate_agent(query: str, tool_calls, response: str) -> dict:
    """Agent-quality metrics (tool-call accuracy + task adherence), 0..1.

    `tool_calls` is the list of tool names/args taken from the agent record
    (e.g. `[{"tool": ..., "args": {...}}, ...]` or a list of tool-name strings).
    Returns a subset of {tool_call_accuracy, task_adherence}.
    """
    if not azure_evals_available():
        return {}
    cfg = model_config()
    out: dict = {}
    try:
        from azure.ai.evaluation import (
            ToolCallAccuracyEvaluator, TaskAdherenceEvaluator,
        )
    except Exception:
        return {}

    normalized_calls = _to_sdk_tool_calls(tool_calls)
    try:
        r = ToolCallAccuracyEvaluator(cfg)(
            query=query, response=response, tool_calls=normalized_calls,
        )
        score = _extract(r, "tool_call_accuracy")
        if score is not None:
            out["tool_call_accuracy"] = score
    except Exception:
        pass
    try:
        r = TaskAdherenceEvaluator(cfg)(query=query, response=response)
        score = _extract(r, "task_adherence")
        if score is not None:
            out["task_adherence"] = score
    except Exception:
        pass
    return out


def _to_sdk_tool_calls(tool_calls) -> list:
    """Coerce the agent record's tool trace into the SDK's tool_call shape.

    Accepts either a list of names (["get_pipeline_status", ...]) or a list of
    dicts ({"tool": name, "args": {...}}) and returns the SDK's expected form.
    """
    calls = []
    for tc in tool_calls or []:
        if isinstance(tc, str):
            name, args = tc, {}
        elif isinstance(tc, dict):
            name = tc.get("tool") or tc.get("name") or ""
            args = tc.get("args") or tc.get("arguments") or {}
        else:
            continue
        calls.append({
            "type": "tool_call",
            "name": name,
            "arguments": args,
        })
    return calls
