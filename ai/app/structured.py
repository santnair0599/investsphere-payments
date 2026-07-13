"""
Structured recommendation path.

Runs the normal agent (raw or LangGraph, per AGENT_RUNTIME) to gather grounded evidence
via tool calls + RAG, then produces a **schema-constrained `BusinessRecommendation`** JSON
using Azure OpenAI structured outputs. Falls back to wrapping the prose answer if structured
parsing is unavailable, so it never raises. Lazy imports — loads without the SDK/creds.
"""
from __future__ import annotations

import json

from ai.app.schemas import BusinessRecommendation


def _fallback(record: dict) -> dict:
    """No structured-output call available → wrap the prose answer in the schema."""
    return BusinessRecommendation(
        summary=(record.get("answer") or "")[:280],
        items=[],
        confidence="MEDIUM" if record.get("trust_checked") else "LOW",
        trust_reasons="derived from prose answer (structured formatting unavailable)",
        citations=[m for m in record.get("retrieved_docs", []) if m],
    ).model_dump()


def answer_structured(question: str, session_id: str = "adhoc",
                      user_id: str = "anonymous") -> dict:
    """Return {record, recommendation} — the traced run plus a structured recommendation."""
    from ai.app.runtime import answer as run_agent
    record = run_agent(question, session_id=session_id, user_id=user_id)

    # Refusals / capacity degradation stay prose — nothing to structure.
    if record.get("safety_status") in ("BLOCKED_INJECTION", "NEEDS_APPROVAL",
                                       "DEGRADED_CAPACITY"):
        return {"record": record, "recommendation": _fallback(record)}

    try:
        from ai.app import agent as raw
        client = raw._client()
        messages = [
            {"role": "system", "content":
                "Convert the analyst answer into the BusinessRecommendation schema. Use ONLY "
                "facts present in the answer; do not invent figures. confidence mirrors the "
                "stated data-trust level."},
            {"role": "user", "content": record.get("answer", "")},
        ]
        # Prefer the SDK's typed parse; fall back to json_schema response_format.
        try:
            completion = client.beta.chat.completions.parse(
                model=raw.MODEL, messages=messages,
                response_format=BusinessRecommendation, temperature=0)
            rec = completion.choices[0].message.parsed
            recommendation = rec.model_dump() if rec else _fallback(record)
        except Exception:
            from ai.app.schemas import RECOMMENDATION_JSON_SCHEMA
            resp = client.chat.completions.create(
                model=raw.MODEL, messages=messages, temperature=0,
                response_format={"type": "json_schema",
                                 "json_schema": RECOMMENDATION_JSON_SCHEMA})
            recommendation = json.loads(resp.choices[0].message.content)
        return {"record": record, "recommendation": recommendation}
    except Exception:
        return {"record": record, "recommendation": _fallback(record)}
