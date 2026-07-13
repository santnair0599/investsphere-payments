"""
Agent runtime selector.

AGENT_RUNTIME picks which runtime serves a request:
  * "raw"       (default) — the dependency-light Azure OpenAI SDK tool-calling loop
                            (ai/app/agent.py). No LangGraph needed.
  * "langgraph"           — the real LangGraph StateGraph (ai/app/agent_langgraph.py).

The chosen module is imported LAZILY, so the raw path never imports LangGraph and the
LangGraph path is only pulled in when explicitly selected. Both return the same record.
"""
from __future__ import annotations

import os

ACTIVE_RUNTIME = os.environ.get("AGENT_RUNTIME", "raw").lower()


def answer(question: str, session_id: str = "adhoc", user_id: str = "anonymous") -> dict:
    if os.environ.get("AGENT_RUNTIME", "raw").lower() == "langgraph":
        from ai.app import agent_langgraph
        return agent_langgraph.answer(question, session_id, user_id)
    from ai.app import agent
    return agent.answer(question, session_id, user_id)
