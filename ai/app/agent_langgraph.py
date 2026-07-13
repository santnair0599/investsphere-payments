"""
Enterprise Decision Agent — REAL LangGraph StateGraph runtime.

Explicit, bounded graph:

    guard_input → agent_llm → tool_router → tool_execute → agent_llm → guard_output → record_run

Selected by AGENT_RUNTIME=langgraph. The raw Azure OpenAI SDK loop in
``ai/app/agent.py`` remains the dependency-light default (AGENT_RUNTIME=raw) and is
NOT removed. This module imports LangGraph at the top on purpose — it genuinely uses
the framework; ``ai/app/runtime.py`` only imports it when the flag selects it.

Everything else is REUSED from the raw runtime (no logic duplicated):
  * tools        — ai.app.agent.ALL_TOOL_SPECS / DISPATCH (business tools + RAG)
  * guardrails   — ai.guardrails.guardrails.check_input / scan_output
  * trust gate   — ai.app.agent._extract_trust (the agent is prompted to call the trust tool)
  * LLM config   — ai.app.agent._client / MODEL / SYSTEM_PROMPT / MAX_TOOL_TURNS
  * observability— ai.app.agent._finish → ai.observability.recorder + pricing (ai_control.*)
  * tracing      — ai.observability.tracing spans
"""
from __future__ import annotations

import json
import time
from typing import TypedDict

from langgraph.graph import StateGraph, START, END

from ai.app import agent as raw
from ai.app import model_router
from ai.guardrails import guardrails as guard
from ai.observability import tracing


class AgentState(TypedDict, total=False):
    question: str
    session_id: str
    user_id: str
    t0: float
    messages: list
    token_by_model: dict
    final_model: str
    routing_reason: str
    tool_trace: list
    turns: int
    draft: str
    final: str
    safety: str
    safety_override: str
    refused: bool
    refusal_status: str
    pending: list
    record: dict


# ---- nodes ------------------------------------------------------------------
def guard_input(state: AgentState) -> dict:
    g = guard.check_input(state["question"])
    if not g.allowed:
        return {"refused": True, "refusal_status": g.status, "draft": raw._refusal(g)}
    return {
        "refused": False,
        "messages": [
            {"role": "system", "content": raw.SYSTEM_PROMPT},
            {"role": "user", "content": state["question"]},
        ],
        "token_by_model": {}, "tool_trace": [], "turns": 0,
    }


def _acc(tok: dict, model: str, resp, span=None) -> None:
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


def agent_llm(state: AgentState) -> dict:
    question = state["question"]
    tok = dict(state.get("token_by_model") or {})
    gather_model, _ = model_router.route(question, "tool_selection")
    synth_model, routing_reason = model_router.route(question, "synthesis")
    try:
        client = raw._client()
        with tracing.span("llm.chat", model=gather_model) as sp:
            resp = client.chat.completions.create(
                model=gather_model, messages=state["messages"],
                tools=raw.ALL_TOOL_SPECS, tool_choice="auto", temperature=0.1)
            _acc(tok, gather_model, resp, sp)
    except Exception as exc:  # rate limit / transient after SDK retries → graceful degrade
        return {"draft": raw._capacity_degrade(exc), "safety_override": "DEGRADED_CAPACITY",
                "pending": [], "token_by_model": tok, "final_model": gather_model,
                "routing_reason": routing_reason}

    msg = resp.choices[0].message
    messages = state["messages"] + [msg.model_dump()]
    if msg.tool_calls:
        pending = [{"id": tc.id, "name": tc.function.name, "args": tc.function.arguments}
                   for tc in msg.tool_calls]
        return {"messages": messages, "pending": pending, "token_by_model": tok}

    # final answer — synthesis upgrade with the reasoning model for complex questions
    draft = msg.content or ""
    final_model = gather_model
    if synth_model != gather_model and draft and not draft.startswith("I could not complete"):
        try:
            with tracing.span("llm.chat", model=synth_model) as sp2:
                sresp = client.chat.completions.create(
                    model=synth_model, temperature=0.1,
                    messages=messages + [{"role": "user", "content": raw.SYNTH_INSTRUCTION}])
                _acc(tok, synth_model, sresp, sp2)
            draft = sresp.choices[0].message.content or draft
            final_model = synth_model
        except Exception:
            pass
    return {"messages": messages, "pending": [], "draft": draft,
            "token_by_model": tok, "final_model": final_model, "routing_reason": routing_reason}


def tool_router(state: AgentState) -> dict:
    """Passthrough node — the branch decision is the conditional edge below."""
    return {}


def tool_execute(state: AgentState) -> dict:
    messages = list(state["messages"])
    trace = list(state.get("tool_trace", []))
    for p in state.get("pending", []):
        args = json.loads(p["args"] or "{}")
        with tracing.span("tool.call", tool=p["name"]) as sp:
            result = raw._run_tool(p["name"], args)
            ok = "error" not in result
            tracing.set_attributes(sp, ok=ok, success=ok)
        trace.append({"tool": p["name"], "args": args, "mart": result.get("mart"), "ok": ok,
                      "evidence": json.dumps(result, default=str)[:2000]})
        messages.append({"role": "tool", "tool_call_id": p["id"],
                         "content": json.dumps(result, default=str)})
    return {"messages": messages, "tool_trace": trace,
            "turns": state.get("turns", 0) + 1, "pending": []}


def guard_output(state: AgentState) -> dict:
    draft = state.get("draft", "") or ""
    gout = guard.scan_output(draft)
    final = gout.redacted_text if gout.status == "BLOCKED_PII" else draft
    safety = state.get("safety_override") or (
        gout.status if gout.status == "BLOCKED_PII" else "PASS")
    return {"final": final, "safety": safety}


def record_run(state: AgentState) -> dict:
    """Reuse the raw runtime's _finish → cost + ai_control.* Delta write."""
    if state.get("refused"):
        rec = raw._finish(state["question"], state.get("draft", ""), [], [],
                          state.get("refusal_status"), state["session_id"],
                          state["user_id"], state["t0"], trust=None)
        return {"record": rec}
    trace = state.get("tool_trace", [])
    retrieved = [t.get("mart") for t in trace if t["tool"] == "search_policy_docs"]
    rec = raw._finish(state["question"], state.get("final", state.get("draft", "")),
                      trace, retrieved, state.get("safety", "PASS"),
                      state["session_id"], state["user_id"], state["t0"],
                      trust=raw._extract_trust(trace),
                      token_by_model=state.get("token_by_model"),
                      model=state.get("final_model"),
                      routing_reason=state.get("routing_reason"))
    return {"record": rec}


# ---- routing ----------------------------------------------------------------
def _after_guard_input(state: AgentState) -> str:
    return "record_run" if state.get("refused") else "agent_llm"


def _after_router(state: AgentState) -> str:
    pending = state.get("pending") or []
    if pending and state.get("turns", 0) < raw.MAX_TOOL_TURNS:
        return "tool_execute"
    return "guard_output"


# ---- graph (built + compiled once) ------------------------------------------
_compiled_graph = None


def _build_graph():
    g = StateGraph(AgentState)
    g.add_node("guard_input", guard_input)
    g.add_node("agent_llm", agent_llm)
    g.add_node("tool_router", tool_router)
    g.add_node("tool_execute", tool_execute)
    g.add_node("guard_output", guard_output)
    g.add_node("record_run", record_run)

    g.add_edge(START, "guard_input")
    g.add_conditional_edges("guard_input", _after_guard_input,
                            {"agent_llm": "agent_llm", "record_run": "record_run"})
    g.add_edge("agent_llm", "tool_router")
    g.add_conditional_edges("tool_router", _after_router,
                            {"tool_execute": "tool_execute", "guard_output": "guard_output"})
    g.add_edge("tool_execute", "agent_llm")
    g.add_edge("guard_output", "record_run")
    g.add_edge("record_run", END)
    return g.compile()


def compiled_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = _build_graph()
    return _compiled_graph


def answer(question: str, session_id: str = "adhoc", user_id: str = "anonymous") -> dict:
    """Run one turn through the LangGraph StateGraph. Same return shape as the raw runtime."""
    graph = compiled_graph()
    recursion_limit = 3 * raw.MAX_TOOL_TURNS + 8   # bounded backstop
    with tracing.span("agent.run.langgraph", session_id=session_id, user_id=user_id,
                      question_length=len(question or "")) as sp:
        result = graph.invoke(
            {"question": question, "session_id": session_id, "user_id": user_id,
             "t0": time.time()},
            config={"recursion_limit": recursion_limit})
        rec = result.get("record", {})
        tracing.set_attributes(sp,
            tools_called=len(rec.get("tools_called", [])),
            tokens_in=rec.get("tokens_in", 0), tokens_out=rec.get("tokens_out", 0),
            estimated_cost=rec.get("estimated_cost", 0),
            safety_status=rec.get("safety_status"), latency_ms=rec.get("latency_ms"))
        return rec
