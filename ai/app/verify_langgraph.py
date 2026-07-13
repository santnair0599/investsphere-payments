"""
Proof that the LangGraph runtime is a REAL compiled StateGraph — not just naming.

    python -m ai.app.verify_langgraph

Exits non-zero if LangGraph isn't genuinely used (import fails, or the compiled object
doesn't come from the langgraph package, or the expected nodes are missing). Runs in CI
after `pip install -r ai/requirements.txt`. Needs NO Azure credentials — it builds and
inspects the graph without invoking the LLM.
"""
from __future__ import annotations

import sys

EXPECTED_NODES = {"guard_input", "agent_llm", "tool_router", "tool_execute",
                  "guard_output", "record_run"}


def main() -> int:
    import langgraph                       # must be installed (real dependency)
    from ai.app import agent_langgraph as lg

    graph = lg.compiled_graph()
    module = type(graph).__module__
    assert module.startswith("langgraph"), f"compiled graph is not from langgraph: {module}"

    nodes = set(lg._build_graph().get_graph().nodes.keys())
    missing = EXPECTED_NODES - nodes
    assert not missing, f"graph missing expected nodes: {missing}"

    print("LangGraph runtime PROVEN real:")
    print(f"  langgraph version : {getattr(langgraph, '__version__', '?')}")
    print(f"  compiled type     : {module}.{type(graph).__name__}")
    print(f"  nodes             : {sorted(n for n in nodes if n in EXPECTED_NODES)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
