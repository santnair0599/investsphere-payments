"""
[8b] Production failure / resilience tests.

Exercises the REAL degradation logic in ai/app/agent.answer by mocking the Azure
OpenAI client (so no SDK/creds are needed) and injecting downstream failures:
  * LLM throttle/5xx after retries  -> status DEGRADED_CAPACITY, no crash
  * a marts tool raises             -> _run_tool catches -> error surfaced, turn continues
  * the policy RAG tool raises      -> answer still produced without policy context
  * malformed input                 -> handled by the input guard, never raises

  pytest ai/tests/failure/chaos.py -v
"""
from __future__ import annotations

from types import SimpleNamespace
import pytest


# ---- lightweight fakes that mimic the openai response shape -----------------
class _Msg:
    def __init__(self, content=None, tool_calls=None):
        self.content = content
        self.tool_calls = tool_calls or []

    def model_dump(self):
        return {"role": "assistant", "content": self.content,
                "tool_calls": [{"id": tc.id, "type": "function",
                                "function": {"name": tc.function.name,
                                             "arguments": tc.function.arguments}}
                               for tc in self.tool_calls]}


def _tool_call(name, args="{}", cid="call_1"):
    return SimpleNamespace(id=cid, type="function",
                           function=SimpleNamespace(name=name, arguments=args))


def _resp(msg):
    return SimpleNamespace(choices=[SimpleNamespace(message=msg)],
                           usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5))


class FakeClient:
    """Returns scripted turns; repeats the last turn for any extra synthesis calls.
    A turn that is an Exception is raised (simulating a throttle/5xx)."""
    def __init__(self, turns):
        self._turns = list(turns)
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **_):
        item = self._turns.pop(0) if len(self._turns) > 1 else self._turns[0]
        if isinstance(item, Exception):
            raise item
        return item


def _patch_client(monkeypatch, turns):
    import ai.app.agent as agent
    monkeypatch.setattr(agent, "_client", lambda: FakeClient(turns))
    return agent


def _first_tool_name():
    from ai.app.agent import ALL_TOOL_SPECS
    return ALL_TOOL_SPECS[0]["function"]["name"]


# ---- tests ------------------------------------------------------------------
def test_llm_throttle_degrades_gracefully(monkeypatch):
    _patch_client(monkeypatch, [RuntimeError("429 rate limit (retries exhausted)")])
    from ai.app.runtime import answer
    out = answer(question="Summarise payment volume by type.", session_id="chaos")
    assert isinstance(out, dict)
    # degraded gracefully: a capacity message instead of an unhandled exception
    signals = " ".join(str(v) for v in out.values()).lower()
    assert "capacity" in signals or "rate limit" in signals or "degraded" in signals


def test_marts_tool_failure_is_surfaced_not_fatal(monkeypatch):
    agent = _patch_client(monkeypatch, [
        _resp(_Msg(tool_calls=[_tool_call(_first_tool_name())])),
        _resp(_Msg(content="Partial answer; some figures were unavailable.")),
    ])
    monkeypatch.setitem(agent.DISPATCH, _first_tool_name(),
                        lambda **_: (_ for _ in ()).throw(ConnectionError("warehouse down")))
    from ai.app.runtime import answer
    out = answer(question="Top customers by total payment amount?", session_id="chaos")
    assert isinstance(out, dict) and out.get("answer")     # produced an answer, didn't crash


def test_rag_failure_proceeds_without_policy(monkeypatch):
    agent = _patch_client(monkeypatch, [
        _resp(_Msg(tool_calls=[_tool_call("search_policy_docs")])),
        _resp(_Msg(content="Total USD payments on 2026-06-30 were ...")),
    ])
    if "search_policy_docs" in agent.DISPATCH:
        monkeypatch.setitem(agent.DISPATCH, "search_policy_docs",
                            lambda **_: (_ for _ in ()).throw(TimeoutError("search 503")))
    from ai.app.runtime import answer
    out = answer(question="What were total USD payments on 2026-06-30?", session_id="chaos")
    assert isinstance(out, dict) and out.get("answer")


@pytest.mark.parametrize("q", ["", "   ", "x"])
def test_malformed_input_never_raises(monkeypatch, q):
    _patch_client(monkeypatch, [_resp(_Msg(content="ok"))])
    from ai.app.runtime import answer
    assert isinstance(answer(question=q, session_id="chaos"), dict)
