"""
Provision (create or update) the Azure AI Foundry agent from ``agent_config.yaml``.

This is the deploy-time twin of the LangGraph/FastAPI agent: it registers the SAME
system prompt, the SAME business-mart tools (as an OpenAPI tool pointing at the
Container App), and an Azure AI Search knowledge tool — all on the managed Foundry
Agent Service runtime.

Order of operations (run this LAST):
  1. Bicep/azd provisions the AI Foundry project + Azure AI Search + Container App.
  2. The Container App image (FastAPI) is deployed and reachable at its FQDN.
  3. `python -m ai.foundry.build_openapi_tools` regenerates openapi_tools.json with
     the real FQDN substituted for REPLACE_WITH_CONTAINER_APP_FQDN.
  4. Run THIS script to create/update the agent against the live project.

Runtime credentials (env vars, only needed for a real run — NOT for --dry-run):
  * AZURE_AI_PROJECT_CONNECTION_STRING  — or —  PROJECT_ENDPOINT
  * AZURE_OPENAI_DEPLOYMENT             — model deployment name (e.g. gpt-4o)
  * AZURE_SEARCH_INDEX                  — policy index for the knowledge tool
Auth uses DefaultAzureCredential (managed identity / az login).

The azure-ai-projects / azure-ai-agents SDK is imported LAZILY inside the code
path that actually calls Azure, so this module imports and `--dry-run` works with
NO SDK and NO credentials installed.

Usage:
    python -m ai.foundry.provision_agent --dry-run     # print resolved definition
    python -m ai.foundry.provision_agent               # create/update in Azure
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "agent_config.yaml"
OPENAPI_PATH = HERE / "openapi_tools.json"


def _expand_env(value):
    """Expand ${VAR} references in YAML scalar values using os.environ."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


def load_agent_definition() -> dict:
    """Resolve agent_config.yaml + system_prompt.md + openapi_tools.json into a
    single, SDK-agnostic definition dict. No Azure calls, no SDK import."""
    import yaml  # pyyaml is a plain dependency (see ai/requirements.txt)

    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))

    instructions_file = (HERE / cfg["instructions_file"]).resolve()
    instructions = instructions_file.read_text(encoding="utf-8")

    tools: list[dict] = []
    for tool in cfg.get("tools", []):
        ttype = tool.get("type")
        if ttype == "openapi":
            spec_path = (HERE / tool["spec"]).resolve()
            openapi_spec = None
            if spec_path.exists():
                openapi_spec = json.loads(spec_path.read_text(encoding="utf-8"))
            tools.append({
                "type": "openapi",
                "name": tool["name"],
                "description": tool.get("description", ""),
                "spec_path": str(spec_path),
                "auth": tool.get("auth", "managed_identity"),
                "spec": openapi_spec,
            })
        elif ttype == "azure_ai_search":
            tools.append({
                "type": "azure_ai_search",
                "name": tool["name"],
                "description": tool.get("description", ""),
                "index": _expand_env(tool.get("index", "${AZURE_SEARCH_INDEX}")),
                "query_type": tool.get("query_type", "semantic"),
                "top_k": tool.get("top_k", 4),
            })
        elif ttype == "content_safety":
            tools.append({"type": "content_safety", "name": tool.get("name", "guardrails")})

    model = _expand_env(cfg["model"])
    if not model or "${" in model:  # unresolved placeholder -> fall back
        model = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")

    return {
        "name": cfg["name"],
        "model": model,
        "description": (cfg.get("description") or "").strip(),
        "instructions": instructions,
        "tools": tools,
        "content_safety": cfg.get("content_safety", {}),
    }


def _print_dry_run(defn: dict) -> None:
    tool_lines = [f"      - {t['type']}: {t.get('name')}" for t in defn["tools"]]
    openapi = next((t for t in defn["tools"] if t["type"] == "openapi"), None)
    op_count = len(openapi["spec"]["paths"]) if openapi and openapi.get("spec") else 0
    print("Resolved Foundry agent definition (dry-run — no Azure call):")
    print(f"  name              : {defn['name']}")
    print(f"  model             : {defn['model']}")
    print(f"  instructions len  : {len(defn['instructions'])} chars")
    print(f"  tool count        : {len(defn['tools'])}")
    print("\n".join(tool_lines))
    print(f"  openapi operations: {op_count}")
    if openapi and not openapi.get("spec"):
        print("  NOTE: openapi_tools.json not found — run "
              "`python -m ai.foundry.build_openapi_tools` first.")
    print(f"  content_safety    : {defn['content_safety']}")


def _provision(defn: dict) -> None:
    """Create/update the agent in Azure. SDK imported lazily HERE only."""
    from azure.identity import DefaultAzureCredential
    from azure.ai.projects import AIProjectClient
    from azure.ai.agents.models import OpenApiTool, OpenApiManagedAuthDetails, AzureAISearchTool

    conn = os.environ.get("AZURE_AI_PROJECT_CONNECTION_STRING")
    endpoint = os.environ.get("PROJECT_ENDPOINT")
    if not conn and not endpoint:
        raise SystemExit(
            "Set AZURE_AI_PROJECT_CONNECTION_STRING or PROJECT_ENDPOINT to provision "
            "(or use --dry-run)."
        )

    credential = DefaultAzureCredential()
    if endpoint:
        project = AIProjectClient(endpoint=endpoint, credential=credential)
    else:
        project = AIProjectClient.from_connection_string(
            conn_str=conn, credential=credential
        )

    agents = project.agents
    tool_defs: list = []

    openapi = next((t for t in defn["tools"] if t["type"] == "openapi"), None)
    if openapi:
        if not openapi.get("spec"):
            raise SystemExit("openapi_tools.json missing — run build_openapi_tools first.")
        tool_defs.extend(
            OpenApiTool(
                name=openapi["name"],
                description=openapi["description"],
                spec=openapi["spec"],
                auth=OpenApiManagedAuthDetails(),  # managed identity
            ).definitions
        )

    search = next((t for t in defn["tools"] if t["type"] == "azure_ai_search"), None)
    if search:
        conn_id = os.environ.get("AZURE_SEARCH_CONNECTION_ID", "")
        tool_defs.extend(
            AzureAISearchTool(
                index_connection_id=conn_id,
                index_name=search["index"],
            ).definitions
        )

    agent = agents.create_agent(
        model=defn["model"],
        name=defn["name"],
        description=defn["description"],
        instructions=defn["instructions"],
        tools=tool_defs,
    )
    print(f"Provisioned Foundry agent '{defn['name']}' -> id={getattr(agent, 'id', '?')}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision the Azure AI Foundry agent.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the resolved definition without calling Azure "
                             "(works with no creds and no SDK).")
    args = parser.parse_args()

    defn = load_agent_definition()
    if args.dry_run:
        _print_dry_run(defn)
        return
    _provision(defn)


if __name__ == "__main__":
    main()
