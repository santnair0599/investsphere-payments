# LLM Patterns in the Enterprise Decision Agent

The seven GenAI patterns this project implements, mapped to concrete code. All are present;
each Azure-native piece is feature-flagged with a deterministic fallback for local/CI.

| # | Pattern | Status | Where |
|---|---|---|---|
| 1 | RAG over business policies + KPI definitions | ✅ | `ai/rag/` |
| 2 | Tool-calling over governed Databricks Gold marts | ✅ | `ai/tools/` |
| 3 | Agentic workflow for business recommendations | ✅ | `ai/app/agent*.py` |
| 4 | Structured JSON outputs for risk + recommendations | ✅ | `ai/app/schemas.py` + `/recommend` |
| 5 | Guardrails for PII + prompt injection | ✅ | `ai/guardrails/` |
| 6 | Evaluation-driven CI/CD (RAG + agent evaluators) | ✅ | `ai/eval/` + CI |
| 7 | Observability (run logs, tool traces, App Insights) | ✅ | `ai/observability/` |

---

### 1. RAG over business policies and KPI definitions
Azure AI Search **hybrid (BM25 + vector/HNSW) with semantic ranking** over 8 curated policy /
KPI Markdown docs (leasing, investment risk, revenue management, maintenance SLA, guest
experience, marketing, governance, KPI definitions).
`ai/rag/retriever.py` (query: `search_text` + `vector_queries` + `query_type="semantic"`),
`ai/rag/index_policies.py` (index: vector + semantic config), `ai/rag/policies/*.md`.

### 2. Tool-calling over governed Databricks Gold marts
8 read-only SQL tools over the `gold_*` marts (+ `gold_ops_trust`), returning **typed JSON rows**.
Read-only, arg-validated, bounded result size, Unity Catalog masked views — the model never
guesses a number. `ai/tools/business_tools.py` (`TOOL_SPECS` / `TOOL_DISPATCH`),
`ai/tools/databricks_client.py` (SELECT-only guard, `MAX_ROWS`).

### 3. Agentic workflow for business decision recommendations
A bounded tool-calling agent that plans which tools to call and in what order, checks the data
**trust gate**, and synthesizes a recommendation. Two interchangeable runtimes (`AGENT_RUNTIME`):
the raw Azure OpenAI SDK loop (`ai/app/agent.py`) and a real **LangGraph `StateGraph`**
(`ai/app/agent_langgraph.py`: `guard_input → agent_llm → tool_router → tool_execute → agent_llm →
guard_output → record_run`). Bounded by `MAX_TOOL_TURNS`; human-in-the-loop for actions.

### 4. Structured JSON outputs for risk and recommendation responses
- **Risk outputs** are already structured JSON — every mart tool returns typed rows
  (`entity`, metrics, `risk_reasons`, flags).
- **Recommendation output** is schema-constrained JSON via **Azure OpenAI structured outputs**:
  `ai/app/schemas.py` defines `BusinessRecommendation` (summary, items[{domain, entity, metrics,
  risk_reasons, recommended_action}], confidence, trust_reasons, citations); `ai/app/structured.py`
  coerces the grounded answer into it (`response_format` / typed `parse`, with a no-creds fallback);
  exposed at **`POST /recommend`** (`response_model=BusinessRecommendation`).

### 5. Guardrails for PII and prompt injection
Deterministic, code-enforced (not left to the LLM): input **prompt-injection / jailbreak** +
write-action-approval screening, output **PII** scan/redaction. Layered with live **Azure Content
Safety Prompt Shields** (feature-flagged) and Unity Catalog masked views.
`ai/guardrails/guardrails.py`.

### 6. Evaluation-driven CI/CD using RAG and agent evaluators
A 30-question golden set scored by **Azure AI Evaluation SDK** evaluators — RAG (groundedness /
relevance / retrieval) + agent (tool-call accuracy / task adherence) — with deterministic
heuristics as fallback. The **CI eval gate blocks deploy** on groundedness / tool-call accuracy /
PII / safety / latency thresholds. `ai/eval/run_evals.py`, `ai/eval/azure_evaluators.py`,
`ai/eval/eval_dataset.json`, `.github/workflows/ai-deploy.yml`.

### 7. Observability using agent run logs, tool-call traces, and App Insights
Durable **Delta trace tables** — `ai_control.agent_runs` (question, answer, tools, tokens, cost,
safety) + `ai_control.agent_tool_calls` (per tool) — **complemented** by **OpenTelemetry →
Application Insights** spans (`agent.run` / `llm.chat` / `tool.call`, latency + errors).
`ai/observability/` (`ddl.sql`, `recorder.py`, `tracing.py`, `pricing.py`).

---

## The one-line summary
> *RAG (hybrid + semantic) for policy grounding · tool calling over governed marts for live
> numbers · an agentic, trust-gated workflow · structured JSON risk + recommendation outputs ·
> deterministic guardrails + Prompt Shields · an eval-gated CI/CD with RAG + agent evaluators ·
> and dual observability (Delta trace tables + App Insights spans).* All seven, feature-flagged
> with fallbacks so they validate locally without Azure credentials.
