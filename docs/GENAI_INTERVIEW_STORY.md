# InvestSphere Enterprise Decision Agent — Interview Story

## The 30-second pitch
> I built an **enterprise business decision agent** on top of a Databricks lakehouse
> for a diversified investment holding company. The lakehouse ingests from **six
> enterprise source patterns** (Autoloader, SFTP, REST, JDBC, Salesforce, Debezium CDC)
> into Bronze, applies cleaning, **SCD2**, **DQ gates**, **quarantine**, and governance
> in Silver, then builds **Gold business marts** for real estate, hospitality,
> entertainment, investment, customer segments, and operational trust. On top, an
> **Azure AI Foundry / LangGraph** agent answers leadership questions — it doesn't
> guess: it **calls SQL tools over the marts**, **RAG's over business policies**,
> **checks pipeline & data-quality trust**, and returns structured recommendations
> with a **confidence level** and cited evidence.

Positioning: **Data + AI Platform Engineer / Azure AI Agent Engineer.**

## Architecture in one breath
```
6 sources → Bronze → Silver (DQ / SCD2 / quarantine / control)
        → Gold marts (realestate, hospitality, entertainment, investment, customer, ops_trust)
        → Databricks SQL Warehouse (governed, read-only, UC masked views)
                    ▲
   Azure AI Foundry / LangGraph agent (Container Apps + FastAPI)
   tools over marts + RAG (Azure AI Search) + guardrails + eval gate + observability
```
Two deployment planes: **Databricks Asset Bundles** own the data; **GitHub Actions +
Bicep** own the AI plane, with a **GenAIOps eval gate** blocking bad deploys.

## Model choice (the 20-second version)
We didn't pick a model on vibes — we built an **eval harness** (groundedness, tool-call
accuracy, latency, cost, safety, Arabic parity) and ran every candidate through the **same
golden set behind identical tools**. **GPT-4o** won on tool-calling + structured output;
since ~70% of turns are cheap lookups we **route those to GPT-4o-mini** and reserve GPT-4o
for synthesis — near-top quality at a fraction of cost/latency. It's **swappable behind the
AI Gateway** (Llama-3.3 / Claude / Jais are a config change) and **re-validated nightly**.
Full rationale + benchmark method: **[MODEL_SELECTION.md](MODEL_SELECTION.md)**.

## Questions the agent answers
- Which real-estate assets are underperforming? *(get_underperforming_properties)*
- Which hotels have revenue risk? *(get_hotel_revenue_risk)*
- Which venues have high footfall but low conversion? *(get_venue_conversion_risk)*
- Which investment assets have rising risk exposure? *(get_investment_risk_exposure)*
- Which customer segments are declining? *(get_declining_customer_segments)*
- Top actions for leadership this week? *(get_top_business_actions)*
- Why did the pipeline go PARTIAL? Can we trust today's numbers?
  *(get_pipeline_status / get_data_quality_trust_score)*

## The two demo moments that land
**1. Business answer with cited drivers**
> "Top 3 needing attention: Property A (occupancy −8%, maintenance +21%), Venue B
> (footfall +15% but conversion −9%), Hotel C (RevPAR −11%). Recommended: review
> pricing for A, campaign–product fit for B, segment mix for C."
Each figure comes from a `mart_*` row's `risk_reasons` — nothing invented.

**2. Trust-gated recommendation** (connects business + data quality + AI trust)
> "Confidence: MEDIUM. Pipeline SUCCESS and Silver DQ gate passed, but the hospitality
> feed is 6% incomplete today, so treat hotel sentiment cautiously."
The agent calls `get_data_quality_trust_score` (from `gold_ops_trust`) **before**
recommending — a production-engineer signal, not a prompt user.

## What makes it "production," not a chatbot (the differentiators)
| Concern | How it's answered |
|---|---|
| Hallucination | tools-only numbers; groundedness in the **eval gate**; refusal when unbacked |
| Retrieval quality | Azure AI Search **hybrid + semantic reranker** over policy/KPI docs |
| Prompt injection / PII | deterministic **guardrails** + Content Safety/Prompt Shields + UC **masked views** |
| Safe deploys | **eval gate** blocks on groundedness<0.80, tool-accuracy<0.85, PII>0, latency |
| Cost/attribution | APIM token metering; per-run token+cost in `ai_control.agent_runs`; **feature-flagged model router** (cheap `gpt-4o-mini` for lookups/tool-selection, `gpt-4o` for synthesis — `docs/CAPACITY_COST.md`) |
| Observability | `ai_control.agent_runs / agent_tool_calls / agent_evaluations / prompt_versions` |
| Governance | read-only SP, masked views, human approval for write/action tools |

## Agent runtimes (accurate wording)
The agent ships **two interchangeable runtimes over one tool/guardrail/trust/observability layer**,
selected by `AGENT_RUNTIME`:
- **`raw`** (default) — a dependency-light **Azure OpenAI SDK** tool-calling loop (`ai/app/agent.py`).
- **`langgraph`** — a **real LangGraph `StateGraph`** (`ai/app/agent_langgraph.py`): explicit bounded
  nodes `guard_input → agent_llm → tool_router → tool_execute → agent_llm → guard_output → record_run`.
  Proven by `python -m ai.app.verify_langgraph` (asserts a `langgraph…CompiledStateGraph` with all
  nodes) — wired into CI.
- The **Azure AI Foundry twin** exposes the same tools as **OpenAPI operations** (`ai/foundry/`),
  the managed-runtime equivalent.
Same `TOOL_DISPATCH`, guardrails, trust gate, and `ai_control.*` recording across all three — no
logic duplicated.

## Evaluation & observability (Azure-native, with fallbacks)
The **Azure deployment uses Azure-native evaluation, tracing, and Prompt Shields when credentials
are configured, with deterministic custom fallbacks for local or CI validation.** Concretely:
Azure AI Evaluation SDK evaluators (groundedness / relevance / retrieval / tool-call accuracy /
task adherence) with the custom heuristic gate as fallback; OpenTelemetry → Application Insights
tracing (`agent.run` / `llm.chat` / `tool.call` spans) complementing the durable `ai_control.*`
Delta tables; and live Content Safety **Prompt Shields** on the input path backed by the regex
guard. Everything is feature-flagged (`EVAL_GATE_ENABLED`, `TRACING_ENABLED`,
`PROMPT_SHIELDS_ENABLED`) — see `docs/EVALUATION_OBSERVABILITY.md`.

## JD → project mapping (UAE Azure AI roles)
Azure AI Foundry ✓ · Azure OpenAI ✓ · Azure AI Search / RAG ✓ · AI agents ✓ ·
tool/function calling ✓ · LangGraph + FastAPI/Pydantic ✓ · vector search ✓ ·
LLM evaluation ✓ · guardrails / Content Safety ✓ · observability ✓ · CI/CD eval gate ✓ ·
enterprise data integration (Databricks lakehouse) ✓ · business impact ✓.

## LLM patterns (all seven, mapped to code)
RAG over policies/KPIs · tool-calling over governed marts · agentic trust-gated workflow ·
**structured JSON risk + recommendation outputs** (`/recommend` → `BusinessRecommendation`) ·
PII + prompt-injection guardrails · eval-driven CI/CD (RAG + agent evaluators) · observability
(Delta trace tables + App Insights spans). Full mapping in **`docs/LLM_PATTERNS.md`**.

## Honest framing (a maturity signal)
The agent **augments leadership; a human stays in the loop.** It recommends, it does
not execute — write/action requests route to human approval. Knowing where GenAI
should *not* be trusted is why the trust/eval layer is the heart of the project.

## "Could you add…?" — documented extensions (answers ready)
When an interviewer probes beyond the build, these are designed, scoped, and *deliberately not
implemented* — each doc doubles as the answer:
- **Fine-tuning?** → `docs/FINE_TUNING_STRATEGY.md` — why RAG + tools is correct here (facts are
  retrieved, not memorized); when fine-tuning helps (behavior, not facts) + JSONL format.
- **Multimodal / vision?** → `docs/MULTIMODAL_EXTENSION.md` — property/venue/invoice imagery as a
  normal Bronze source or a governed VLM tool, with privacy guardrails first.
- **MCP?** → `docs/MCP_EXTENSION.md` — the same governed tools exposed to MCP hosts via `ai/mcp/`,
  separate from the Foundry/FastAPI runtime, no logic duplicated.
- **Multi-agent?** → `docs/MULTI_AGENT_EXTENSION.md` — Decision + DataOps + Router split the tool
  layer already supports; single-agent stays the production choice for control.
