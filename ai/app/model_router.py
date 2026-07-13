"""
Lightweight, feature-flagged model router (cost / latency optimization).

MODEL_ROUTER_ENABLED=true routes each LLM call to a cheap **FAST** deployment or a
**REASONING** deployment, based on question intent + turn kind:
  * FAST      — simple policy lookups, ops-trust / pipeline-status / DQ questions, and
                the tool-selection turns of the loop
  * REASONING — complex business-risk synthesis, cross-domain recommendations,
                executive summaries, and final recommendation generation

Safe by default:
  * MODEL_ROUTER_ENABLED=false            -> always AZURE_OPENAI_DEPLOYMENT (unchanged)
  * AZURE_OPENAI_DEPLOYMENT_FAST unset    -> fall back to AZURE_OPENAI_DEPLOYMENT

Pure function + env reads — imports and unit-tests with no SDK and no credentials.
"""
from __future__ import annotations

import os
import re

# simple intent → cheap lookups / ops / status / policy / DQ
_SIMPLE = re.compile(
    r"\b(pipeline|partial|status|freshness|quarantin\w*|data quality|dq|trust|"
    r"definition|define|what is|according to|policy|sla|kpi|which sources|run[_ ]?date|fresh)\b",
    re.IGNORECASE)
# complex intent → synthesis / recommendation / cross-domain / executive
_COMPLEX = re.compile(
    r"\b(recommend\w*|action\w*|underperform\w*|revenue risk|rising risk|declin\w*|"
    r"conversion|top \d+|attention|leadership|executive|summar\w*|"
    r"across (all )?(the )?(business|domains|units)|synthesi\w*|prioriti\w*|what should)\b",
    re.IGNORECASE)


def _default() -> str:
    return os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")


def enabled() -> bool:
    return os.environ.get("MODEL_ROUTER_ENABLED", "false").lower() in ("1", "true", "yes", "on")


def is_simple(question: str) -> bool:
    """Simple (fast-model) intent = matches a lookup/ops pattern and is NOT a synthesis ask."""
    q = question or ""
    if _COMPLEX.search(q):
        return False
    return bool(_SIMPLE.search(q))


def route(question: str, turn_kind: str = "synthesis") -> tuple[str, str]:
    """Return (deployment_name, routing_reason).

    turn_kind: "tool_selection" for the evidence-gathering loop calls, "synthesis" for
    the final recommendation generation (default).
    """
    default = _default()
    if not enabled():
        return default, "router_disabled"
    fast = os.environ.get("AZURE_OPENAI_DEPLOYMENT_FAST")
    reasoning = os.environ.get("AZURE_OPENAI_DEPLOYMENT_REASONING") or default
    if not fast:
        return default, "fast_unset_fallback"
    if is_simple(question):
        return fast, "simple_lookup->fast"
    # complex question
    if turn_kind == "tool_selection":
        return fast, "complex_tool_selection->fast"
    return reasoning, "complex_synthesis->reasoning"
