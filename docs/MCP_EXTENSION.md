# MCP Extension — optional interoperability layer

An **optional** Model Context Protocol (MCP) server that exposes the InvestSphere business
tools to MCP-compatible hosts (Claude Desktop, IDEs, other agent frameworks).

> **MCP is not required for the production runtime.** The deployed agent uses **Azure AI
> Foundry + FastAPI OpenAPI tools** (`ai/app/`, `ai/foundry/`). The MCP server is a separate,
> additive interoperability surface — same governed tools, a second protocol. You can ignore it
> entirely and the product is unchanged.

## Why it exists
The tools are valuable outside the Azure agent too. MCP is a model-agnostic, host-agnostic
standard: one server, many hosts. Exposing the read-only business tools over MCP means an
MCP-capable client can ask the same governed questions ("which hotels have revenue risk?")
without going through the Azure runtime. It's a **breadth** signal — the same governed tool
layer, reachable two ways.

## One tool definition → two interfaces (no duplication)
```
ai/tools/business_tools.py :: TOOL_DISPATCH   (+ rag.retriever.search_policy_docs)
        │  the single source of truth for tool logic + schemas
        ├─────────────►  FastAPI /tools/* + OpenAPI  →  Azure AI Foundry / Azure OpenAI   [PRODUCTION]
        └─────────────►  ai/mcp/ (MCP stdio server)  →  Claude Desktop / MCP hosts         [OPTIONAL]
```
`ai/mcp/tools.py` **reuses `TOOL_DISPATCH` verbatim** (verified by identity check) — the MCP
surface can never drift from the Foundry/FastAPI one, and there is no second copy of any query.

## What it exposes
**Read-only business tools (9)** — the exact production tool set:
`get_underperforming_properties` · `get_hotel_revenue_risk` · `get_venue_conversion_risk` ·
`get_investment_risk_exposure` · `get_declining_customer_segments` · `get_top_business_actions` ·
`get_pipeline_status` · `get_data_quality_trust_score` · `search_policy_docs`

**Write/action tools (2) — stubs only, human-approval-required:**
`create_action_recommendation` · `update_investigation_status`
- **Disabled by default** (`MCP_ENABLE_ACTIONS=false`).
- Even when enabled, they **never execute a business change** — they return
  `status: REQUIRES_HUMAN_APPROVAL, executed: false`. This mirrors the agent's human-in-the-loop
  rule and demonstrates the MCP *action* pattern without taking real action.

## Files
- `ai/mcp/tools.py` — registry + dispatch; **pure Python**, imports with **no `mcp` SDK and no
  credentials** (the underlying tools import Databricks/Azure drivers lazily at call time). This is
  the CI wiring-check target.
- `ai/mcp/server.py` — the MCP stdio server (`mcp` SDK), thin wrapper over the registry.

## Run it
```bash
pip install mcp                      # optional SDK; not in the prod image by default
python -m ai.mcp.server              # stdio transport
```
Register it with an MCP host (example client config):
```json
{
  "mcpServers": {
    "investsphere": {
      "command": "python",
      "args": ["-m", "ai.mcp.server"],
      "env": {
        "DATABRICKS_HOST": "...", "DATABRICKS_HTTP_PATH": "...", "DATABRICKS_TOKEN": "...",
        "DATABRICKS_CATALOG": "investsphere_prod",
        "AZURE_OPENAI_ENDPOINT": "...", "AZURE_OPENAI_API_KEY": "...",
        "AZURE_SEARCH_ENDPOINT": "...", "AZURE_SEARCH_KEY": "..."
      }
    }
  }
}
```
Credentials are only needed at **call time** (same env the business tools use). Importing the
registry for tests needs none.

## Governance is inherited, not re-implemented
Because MCP calls the same functions, it inherits **all** the existing controls: read-only
service principal, bounded result size, SELECT-only guard, Unity Catalog masked views, and the
human-approval gate on actions. The MCP layer adds **no new data access** — it's a different door
to the same governed room.

## Relationship to Palantir-style patterns
The two action stubs are the "governed action" shape used in ontology/enterprise-agent systems:
in a real Palantir-style deployment they would map to **Ontology object queries + governed
Actions**. Here they are explicit, approval-gated stubs — the pattern, not a live integration.

## Not in scope
No production traffic flows through MCP; it is not deployed by the Bicep/CI pipeline; the two
action tools perform no writes. It's an interoperability convenience, kept deliberately separate
from the Azure runtime.
