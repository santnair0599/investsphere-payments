"""
MCP tool registry — the single place that maps MCP tool names to the EXISTING
business tool functions. No business logic lives here; it reuses
`ai.tools.business_tools.TOOL_DISPATCH` + `ai.rag.retriever.search_policy_docs`
verbatim, so the MCP interface can never drift from the Foundry / FastAPI one.

Pure Python — imports without the `mcp` SDK and without any Azure/Databricks
credentials (the underlying tools import their drivers lazily, at call time). That
lets `import ai.mcp.tools` run in CI as a wiring check.

Read-only by default. Two write/action tools are exposed only as **stubs**: they
are disabled unless `MCP_ENABLE_ACTIONS=true`, and even then they NEVER execute a
business change — they return a "queued for human approval" record. This mirrors the
human-in-the-loop rule in the agent's system prompt.
"""
from __future__ import annotations

import os

# Reuse the canonical tool functions + schemas (do NOT redefine them here).
from ai.tools.business_tools import TOOL_SPECS, TOOL_DISPATCH
from ai.rag.retriever import RAG_TOOL_SPEC, search_policy_docs

ACTIONS_ENABLED = os.environ.get("MCP_ENABLE_ACTIONS", "false").lower() == "true"


def _mcp_tool(openai_spec: dict) -> dict:
    """Convert an OpenAI/Foundry function spec to an MCP tool definition.
    (The `parameters` object already IS a JSON Schema — MCP's `inputSchema`.)"""
    fn = openai_spec["function"]
    return {
        "name": fn["name"],
        "description": fn["description"],
        "inputSchema": fn.get("parameters", {"type": "object", "properties": {}}),
    }


# ---- read-only tools: reuse business tools + the RAG tool --------------------
READONLY_TOOLS = [_mcp_tool(s) for s in TOOL_SPECS] + [_mcp_tool(RAG_TOOL_SPEC)]
READONLY_DISPATCH = {**TOOL_DISPATCH, "search_policy_docs": search_policy_docs}

# ---- write/action tools: STUBS only (human-approval-required / disabled) -----
ACTION_TOOLS = [
    {
        "name": "create_action_recommendation",
        "description": ("[WRITE — disabled by default] Draft a business action recommendation "
                        "for a flagged asset/segment and queue it for HUMAN APPROVAL. Never "
                        "executes a business change. Enable with MCP_ENABLE_ACTIONS=true; even "
                        "then it only records an approval request."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "domain": {"type": "string", "description": "real_estate/hospitality/entertainment/investment/customer"},
                "entity_id": {"type": "string", "description": "e.g. property_id / hotel_id / asset_id"},
                "recommendation": {"type": "string", "description": "the proposed action"},
            },
            "required": ["domain", "entity_id", "recommendation"],
        },
    },
    {
        "name": "update_investigation_status",
        "description": ("[WRITE — disabled by default] Update the status of an investigation/case. "
                        "Never executes directly; queues the change for HUMAN APPROVAL. "
                        "Enable with MCP_ENABLE_ACTIONS=true (still approval-gated)."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "investigation_id": {"type": "string"},
                "status": {"type": "string", "description": "OPEN / IN_REVIEW / ACTIONED / DISMISSED"},
                "note": {"type": "string"},
            },
            "required": ["investigation_id", "status"],
        },
    },
]
_ACTION_NAMES = {t["name"] for t in ACTION_TOOLS}


def list_tools() -> list[dict]:
    """All MCP tool definitions (read-only tools + action stubs)."""
    return READONLY_TOOLS + ACTION_TOOLS


def _handle_action(name: str, arguments: dict) -> dict:
    """Action tools NEVER execute a business change from the agent. They queue an
    approval request and return it, whether or not actions are 'enabled'."""
    return {
        "tool": name,
        "executed": False,
        "status": "REQUIRES_HUMAN_APPROVAL",
        "actions_enabled": ACTIONS_ENABLED,
        "arguments": arguments,
        "message": ("This is a write/action tool. The agent does not execute business "
                    "changes; this request is queued for human approval and audit."),
    }


def call_tool(name: str, arguments: dict | None = None) -> dict:
    """Dispatch an MCP tool call to the existing read-only function, or to the
    human-approval stub for action tools. Errors are returned, not raised, so the
    MCP host can surface them to the model."""
    arguments = arguments or {}
    if name in READONLY_DISPATCH:
        try:
            return READONLY_DISPATCH[name](**arguments)
        except Exception as exc:  # surfaced to the host/model, not fatal
            return {"error": str(exc), "tool": name}
    if name in _ACTION_NAMES:
        return _handle_action(name, arguments)
    return {"error": f"unknown tool: {name}"}
