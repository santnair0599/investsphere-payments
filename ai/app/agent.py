"""
Enterprise Decision Agent — **raw Azure OpenAI SDK** tool-calling runtime (the default,
dependency-light runtime; AGENT_RUNTIME=raw). The real LangGraph StateGraph equivalent is
``ai/app/agent_langgraph.py`` (AGENT_RUNTIME=langgraph); both are dispatched by
``ai/app/runtime.py`` and reuse the same tools / guardrails / trust gate / observability.

Flow:  guard_input → LLM+tools loop (bounded by MAX_TOOL_TURNS) → guard_output → record
The agent reasons and calls the business tools (SQL over governed marts) + RAG. Guardrails
run deterministically before the model sees input and before the answer leaves. Every turn
is traced to ai_control.agent_runs / agent_tool_calls (+ OpenTelemetry spans).

Runtime deps: openai, databricks-sql-connector. Azure OpenAI creds + Databricks token come
from Key Vault via the Container App's managed identity (env vars).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from ai.guardrails import guardrails as guard
from ai.observability import tracing
from ai.app import model_router
from ai.tools.business_tools import TOOL_SPECS, TOOL_DISPATCH
from ai.rag.retriever import search_policy_docs, RAG_TOOL_SPEC

SYSTEM_PROMPT = (Path(__file__).parent / "system_prompt.md").read_text(encoding="utf-8")
MODEL = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
MAX_TOOL_TURNS = 5

# Reused by both runtimes for the reasoning-model synthesis pass (model router).
SYNTH_INSTRUCTION = (
    "Using ONLY the evidence above, write the final executive recommendation: ranked "
    "items with their risk_reasons, recommended actions, and a confidence line from the "
    "data-trust score. No new facts.")

# full tool surface = business marts + RAG
ALL_TOOL_SPECS = TOOL_SPECS + [RAG_TOOL_SPEC]
DISPATCH = {**TOOL_DISPATCH, "search_policy_docs": search_policy_docs}


def _client():
    """Azure OpenAI client (lazy import so module loads without the SDK).

    ``max_retries`` makes the SDK retry 429 (rate limit) and 5xx with exponential
    backoff, honouring the ``Retry-After`` header — so transient throttling is handled
    transparently. See docs/CAPACITY_COST.md for the quota / rate-limit / cost model.
    """
    from openai import AzureOpenAI
    return AzureOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-10-21"),
        max_retries=int(os.environ.get("AZURE_OPENAI_MAX_RETRIES", "4")),
        timeout=float(os.environ.get("AZURE_OPENAI_TIMEOUT", "30")),
    )


def _run_tool(name: str, args: dict) -> dict:
    fn = DISPATCH.get(name)
    if fn is None:
        return {"error": f"unknown tool {name}"}
    try:
        return fn(**(args or {}))
    except Exception as exc:  # surfaced to the model so it can recover/caveat
        return {"error": str(exc)}


def answer(question: str, session_id: str = "adhoc", user_id: str = "anonymous") -> dict:
    """Run one agent turn end-to-end. Returns the traced result dict.

    The whole turn is wrapped in an ``agent.run`` OpenTelemetry span (a no-op unless
    tracing is enabled — see ``ai.observability.tracing``); each LLM call and tool
    invocation opens a child span. This is live distributed tracing that complements
    the durable Delta trace written by ``ai.observability.recorder``.
    """
    t0 = time.time()
    tool_trace: list[dict] = []

    with tracing.span("agent.run", session_id=session_id, user_id=user_id,
                      question_length=len(question or "")) as run_span:

        # 1) input guardrail (injection / action-approval)
        gin = guard.check_input(question)
        if not gin.allowed:
            return _stamp_run_span(run_span, _finish(
                question, _refusal(gin), [], [], gin.status,
                session_id, user_id, t0, trust=None))

        client = _client()
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ]
        # model routing (safe default: disabled → gather==synth==AZURE_OPENAI_DEPLOYMENT)
        gather_model, _ = model_router.route(question, "tool_selection")
        synth_model, routing_reason = model_router.route(question, "synthesis")
        tok: dict = {}   # per-model token usage → cost attribution

        def _track(model, resp, span=None):
            u = getattr(resp, "usage", None)
            if u is None:
                return
            pin = getattr(u, "prompt_tokens", 0) or 0
            pout = getattr(u, "completion_tokens", 0) or 0
            io = tok.setdefault(model, [0, 0])
            io[0] += pin
            io[1] += pout
            if span is not None:
                tracing.set_attributes(span, prompt_tokens=pin, completion_tokens=pout)

        # 2) tool-calling loop — gather evidence with the (fast) gather model
        for _ in range(MAX_TOOL_TURNS):
            try:
                with tracing.span("llm.chat", model=gather_model) as llm_span:
                    resp = client.chat.completions.create(
                        model=gather_model, messages=messages, tools=ALL_TOOL_SPECS,
                        tool_choice="auto", temperature=0.1,
                    )
                    _track(gather_model, resp, llm_span)
            except Exception as exc:  # rate limit / transient error AFTER SDK retries exhausted
                tracing.set_attributes(run_span, error=True, error_type=type(exc).__name__)
                return _stamp_run_span(run_span, _finish(
                    question, _capacity_degrade(exc), tool_trace, [], "DEGRADED_CAPACITY",
                    session_id, user_id, t0, trust=None, token_by_model=tok,
                    model=gather_model, routing_reason=routing_reason))
            msg = resp.choices[0].message
            if not msg.tool_calls:
                draft = msg.content or ""
                break
            messages.append(msg.model_dump())
            for tc in msg.tool_calls:
                args = json.loads(tc.function.arguments or "{}")
                with tracing.span("tool.call", tool=tc.function.name) as tool_span:
                    result = _run_tool(tc.function.name, args)
                    ok = "error" not in result
                    tracing.set_attributes(tool_span, ok=ok, success=ok)
                    if not ok:
                        tracing.set_attributes(tool_span, error=True,
                                               error_message=str(result.get("error")))
                tool_trace.append({"tool": tc.function.name, "args": args,
                                   "mart": result.get("mart"), "ok": ok,
                                   "evidence": json.dumps(result, default=str)[:2000]})
                messages.append({"role": "tool", "tool_call_id": tc.id,
                                 "content": json.dumps(result, default=str)})
        else:
            draft = "I could not complete the analysis within the tool-call budget."

        # 2b) synthesis upgrade — reasoning model writes the final answer for complex Qs.
        #     Only fires when routing chose a different synth model (never on the default/simple path).
        final_model = gather_model
        if synth_model != gather_model and draft and not draft.startswith("I could not complete"):
            try:
                with tracing.span("llm.chat", model=synth_model) as sspan:
                    sresp = client.chat.completions.create(
                        model=synth_model, temperature=0.1,
                        messages=messages + [{"role": "user", "content": SYNTH_INSTRUCTION}])
                    _track(synth_model, sresp, sspan)
                draft = sresp.choices[0].message.content or draft
                final_model = synth_model
            except Exception:  # keep the gather-model draft on any failure
                pass

        # 3) output guardrail (PII scan → redact/block)
        gout = guard.scan_output(draft)
        final = gout.redacted_text if gout.status == "BLOCKED_PII" else draft
        safety = gout.status if gout.status == "BLOCKED_PII" else "PASS"

        retrieved = [t for t in tool_trace if t["tool"] == "search_policy_docs"]
        return _stamp_run_span(run_span, _finish(
            question, final, tool_trace,
            [r.get("mart") for r in retrieved], safety,
            session_id, user_id, t0, trust=_extract_trust(tool_trace),
            token_by_model=tok, model=final_model, routing_reason=routing_reason))


def _stamp_run_span(run_span, record: dict) -> dict:
    """Copy the finished run's summary metrics onto the ``agent.run`` span, then return
    the record unchanged. No-ops on the dummy span when tracing is disabled."""
    tracing.set_attributes(
        run_span,
        tools_called=len(record.get("tools_called", [])),
        tokens_in=record.get("tokens_in", 0),
        tokens_out=record.get("tokens_out", 0),
        estimated_cost=record.get("estimated_cost", 0),
        safety_status=record.get("safety_status"),
        latency_ms=record.get("latency_ms"),
    )
    return record


def _refusal(g: guard.GuardResult) -> str:
    if g.status == "NEEDS_APPROVAL":
        return ("This is a business action that requires human approval. I can prepare "
                "the recommendation and route it for sign-off, but I won't execute it.")
    return ("I can't help with that request. " + "; ".join(g.reasons))


def _extract_trust(trace: list[dict]) -> str | None:
    return "checked" if any(t["tool"] == "get_data_quality_trust_score" for t in trace) else None


def _capacity_degrade(exc: Exception) -> str:
    """Graceful message when Azure OpenAI is throttling past the retry budget."""
    name = type(exc).__name__
    if "RateLimit" in name or "429" in str(exc):
        return ("The assistant is temporarily at capacity (rate limit). Your request was not "
                "lost — please retry in a few moments. Capacity is being scaled.")
    return ("The assistant is temporarily unavailable. Please retry shortly. "
            f"({name})")


def _finish(question, answer_text, tool_trace, retrieved, safety,
            session_id, user_id, t0, trust, tokens_in=0, tokens_out=0,
            token_by_model=None, model=None, routing_reason=None):
    from ai.observability.pricing import estimate_cost
    model = model or MODEL
    if token_by_model:  # per-model cost (router may use >1 model per run)
        tokens_in = sum(io[0] for io in token_by_model.values())
        tokens_out = sum(io[1] for io in token_by_model.values())
        estimated_cost = round(sum(estimate_cost(m, io[0], io[1])
                                   for m, io in token_by_model.items()), 6)
    else:
        estimated_cost = estimate_cost(model, tokens_in, tokens_out)
    latency_ms = int((time.time() - t0) * 1000)
    record = {
        "question": question,
        "answer": answer_text,
        "tools_called": [t["tool"] for t in tool_trace],
        "retrieved_docs": retrieved,
        "safety_status": safety,
        "latency_ms": latency_ms,
        "trust_checked": trust,
        "session_id": session_id,
        "user_id": user_id,
        "model": model,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "estimated_cost": estimated_cost,
        "routing_reason": routing_reason,
    }
    # live SAMPLED groundedness monitoring (feature-flagged; never breaks the request).
    # Only PASS answers, only ~sample_rate of them; adds groundedness_score +
    # hallucination_flag + evaluation_mode='live_sampled' to the run.
    if safety == "PASS":
        try:
            from ai.observability import live_eval
            if live_eval.should_sample():
                ctx = "\n".join(t.get("evidence", "") for t in tool_trace if t.get("evidence"))
                res = live_eval.evaluate_live(question, answer_text, ctx)
                if res:
                    record.update(res)
        except Exception:
            pass
    # fire-and-forget trace write (best-effort; never breaks the response)
    try:
        from ai.observability.recorder import record_run
        record_run(record, tool_trace)
    except Exception:
        pass
    return record
