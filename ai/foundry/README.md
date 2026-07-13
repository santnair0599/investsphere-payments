# Azure AI Foundry twin

The **managed-runtime twin** of the LangGraph/FastAPI agent. Same system prompt, same
tool surface, same governed Gold marts — a second runtime over one business logic layer,
not a second implementation of it.

Which runtime serves a request is decided by `AGENT_RUNTIME` (`raw` | `langgraph`). The
Foundry agent is a *third* front end: it is provisioned in Azure AI Foundry Agent Service
and calls the very same FastAPI tool operations over OpenAPI.

```
                    ai/tools/business_tools.py   (9 tools, read-only, over Gold)
                    ai/guardrails/ · trust gate · ai_control observability
                                   ▲            ▲
              ┌────────────────────┘            └────────────────────┐
   ai/app/agent.py (raw)                              Foundry Agent Service
   ai/app/agent_langgraph.py (StateGraph)             (agent_config.yaml)
              │                                                      │
              └──────────► FastAPI: POST /tools/<tool> ◄────────────┘
                           (ai/app/tools_router.py)
```

## Files

| File | What it is |
|---|---|
| `agent_config.yaml` | The Foundry Agent Service definition: model, instructions, tools. |
| `openapi_tools.json` | OpenAPI spec for the 8 mart tools. **Generated** — do not hand-edit. |
| `build_openapi_tools.py` | Regenerates `openapi_tools.json` from `TOOL_SPECS`. |
| `provision_agent.py` | Creates/updates the agent in Foundry via the Azure AI Projects SDK. |

The instructions come from `../app/system_prompt.md` — the same file the raw and
LangGraph runtimes load, so the three cannot drift.

## Regenerate the tool spec

`openapi_tools.json` is derived from `ai/tools/business_tools.TOOL_SPECS`. Adding or
changing a tool means regenerating it, or the Foundry twin silently keeps calling the old
surface:

```bash
python -m ai.foundry.build_openapi_tools
```

`tests/test_api_surface.py` asserts every dispatchable tool has a matching
`POST /tools/<name>` operation, so a spec that drifts from the code fails CI.

## Provision

Needs an Azure AI Foundry project and the Container App already deployed (the OpenAPI tool
points at its FQDN).

```bash
export AZURE_AI_PROJECT="<project-connection-string>"
export AZURE_OPENAI_DEPLOYMENT="gpt-4o"
export AZURE_SEARCH_INDEX="investsphere-policies"
export AGENT_BASE_URL="https://<agent-fqdn>"      # from the Bicep `agentFqdn` output

python -m ai.foundry.provision_agent
```

Auth to the tool operations is `managed_identity`: the Foundry agent calls the Container
App as its managed identity. No keys are exchanged.

## What this twin does and does not prove

It demonstrates that the tool layer is runtime-agnostic and that the same governed surface
can be driven by an Azure-managed agent. It does **not** re-implement the trust gate,
guardrails, or observability — those live behind the tool boundary and apply identically no
matter which runtime calls them.

The eval gate (`ai/eval/run_evals.py`) scores the runtime selected by `AGENT_RUNTIME`. It
does not score the Foundry-hosted agent; that is exercised by the nightly live gate.
