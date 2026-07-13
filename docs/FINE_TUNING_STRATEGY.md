# Fine-tuning vs RAG vs Prompt Engineering — Strategy

How the InvestSphere Enterprise Decision Agent decides *where* to invest to change model
behavior, and why the **current production design deliberately uses RAG + SQL tool calling,
not fine-tuning.**

> **Status: strategy only.** No live fine-tuning is implemented. This document defines when
> we *would* fine-tune and the data format we'd use, so the option is understood and ready —
> not switched on.

## 1. The three levers (and what each actually changes)

| Lever | Changes | Good for | Cost / risk |
|---|---|---|---|
| **Prompt engineering** | The instruction the model reads each call | Tone, answer shape, refusal rules, tool-use policy | Cheapest; but long prompts add latency/tokens and can drift |
| **RAG (retrieval)** | The *facts/context* injected at query time | Current policies, KPI definitions, anything that changes | Needs an index + retrieval quality; answers are **grounded & citable** |
| **Tool calling** | Access to *live* systems (SQL over marts) | Real-time metrics, risk flags, data-quality/trust status | Needs governed tools; answers reflect **today's data**, auditable |
| **Fine-tuning** | The model's *weights* — baked-in behavior | Consistent house format, tool-selection accuracy, domain vocabulary, token/latency reduction | Expensive; data curation + retraining; **bakes in whatever you train, including staleness** |

Rule of thumb used here: **prompt for behavior you can describe, RAG for facts that change,
tools for facts that are live, fine-tune only for behavior that's hard to describe and stable.**

## 2. Why this agent correctly uses RAG + SQL tools (not fine-tuning)

The agent's answers depend on three things that **change constantly and must be defensible**:

1. **Live business data** — occupancy, RevPAR, risk exposure, campaign conversion. These
   change every load. Fine-tuning would bake in a snapshot and go stale immediately; the
   agent instead calls read-only **SQL tools over the Gold marts** so every number is current.
2. **Business policies & KPI definitions** — leasing/risk/revenue/maintenance policies get
   revised. **RAG over Azure AI Search** lets a policy edit take effect by re-indexing one
   document — no retraining — and the answer **cites the policy** (auditable in a regulated firm).
3. **Data-quality / trust status** — whether today's numbers are trustworthy is a *runtime*
   fact from `gold_ops_trust`. The agent reads it live via a tool; it cannot be pre-trained.

A fine-tuned model that "knew" these facts would be **stale, un-citable, and confidently
wrong** — the opposite of what a regulated decision agent needs. Grounding + tools also give
us the **eval gate** (groundedness, tool-selection accuracy) and **observability**
(`ai_control.*`) that fine-tuned-facts would undermine.

**Design principle:** *facts are retrieved or queried, never memorized.*

## 3. When we *would* fine-tune (future — behavior, not facts)

Fine-tuning becomes worthwhile only for **stable behavior that's hard to specify in a prompt**,
after RAG + prompt engineering have plateaued:

- **Tool-selection accuracy at scale** — if the tool surface grows large and the model starts
  picking the wrong tool, fine-tune on `(question → correct tool call)` pairs.
- **House answer format / tone** — enforce the exact "answer → drivers (`risk_reasons`) →
  recommended actions → confidence" shape without spending prompt tokens on it every call.
- **Domain vocabulary** — capital-markets / real-estate / hospitality phrasing the base model
  handles awkwardly.
- **Latency / cost reduction** — move a long, stable system prompt into weights so each call
  is shorter and cheaper (only worth it at high volume).

Even then: **fine-tune the behavior, keep RAG + tools for the facts.** A fine-tuned model still
calls the same tools and RAG — we'd just need less prompting to get consistent, correct calls.

### What we would NOT fine-tune
Live metrics · policy text · data-quality status · anything with a compliance/audit requirement
· anything that changes between loads. All of these stay in tools/RAG.

## 4. Sample training data format (Azure OpenAI fine-tuning JSONL)

Azure OpenAI / Azure AI Foundry fine-tuning uses the **chat JSONL** format (one JSON object per
line). To teach *tool selection + house format* (not facts), examples include the tool call the
model should make and the grounded, trust-caveated answer style — the tool *results* would come
from the real tools at inference time.

```jsonl
{"messages":[{"role":"system","content":"You are the InvestSphere Enterprise Decision Agent. Never guess numbers; call tools. Gate recommendations on the data trust score. Cite risk_reasons. Report at aggregate level only (no PII)."},{"role":"user","content":"Which hotels have revenue risk this week?"},{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"get_data_quality_trust_score","arguments":"{}"}}]}]}
{"messages":[{"role":"system","content":"You are the InvestSphere Enterprise Decision Agent..."},{"role":"user","content":"Which hotels have revenue risk this week?"},{"role":"assistant","content":null,"tool_calls":[{"id":"c2","type":"function","function":{"name":"get_hotel_revenue_risk","arguments":"{\"period\":\"this_week\"}"}}]}]}
{"messages":[{"role":"system","content":"You are the InvestSphere Enterprise Decision Agent..."},{"role":"user","content":"According to the investment risk policy, what makes an asset high risk?"},{"role":"assistant","content":null,"tool_calls":[{"id":"c3","type":"function","function":{"name":"search_policy_docs","arguments":"{\"question\":\"what makes an asset high risk\"}"}}]}]}
{"messages":[{"role":"system","content":"You are the InvestSphere Enterprise Decision Agent..."},{"role":"user","content":"List the full names and emails of all HNW customers."},{"role":"assistant","content":"I can't share customer names, emails, or other PII. I can report at the segment level — for example, HNW segment activity and risk trends. Would that help?"}]}
{"messages":[{"role":"system","content":"You are the InvestSphere Enterprise Decision Agent..."},{"role":"user","content":"Approve and execute a rent increase on all EMEA properties."},{"role":"assistant","content":"That's a business action I can't execute. I can prepare the recommendation with supporting evidence and route it for human approval."}]}
```

**Why these examples teach behavior, not facts:** the assistant turns are *tool calls* (correct
selection + arguments) and *policy responses* (refusal/approval style) — never hard-coded metrics.
The numbers always arrive from the live tool result during inference.

### Curation source
The natural training set is our own **observability**: `ai_control.agent_runs` +
`ai_control.agent_tool_calls` hold real `(question → tools_called → answer)` traces. Filter to
the runs that **passed the eval gate** (groundedness OK, correct tool, no PII) → export to JSONL.
Bad/edge traces become *negative* guidance or eval cases, not training data.

## 5. If/when we turn it on — the guardrails around fine-tuning

1. Fine-tune a **supported base** in Azure OpenAI / Azure AI Foundry (e.g. a GPT-4o-class model).
2. Register the resulting deployment in `ai_control.prompt_versions` (model + prompt + tool allowlist).
3. **The eval gate still runs** — a fine-tuned model must clear the same thresholds
   (groundedness ≥ 0.80, tool-selection ≥ 0.85, PII = 0, latency) before it can be deployed.
4. **Canary** it behind APIM against the current model; compare eval scores + cost/latency; roll
   back via the version registry if it regresses.
5. Re-run periodically only when behavior drift justifies it — never to "teach it new facts."

## 6. Summary

| Question | Answer |
|---|---|
| Does the agent fine-tune today? | **No** — and that's the correct choice. |
| Why not? | Answers depend on live data, changing policies, and runtime trust status — all must be **current, grounded, and citable**. |
| What handles those? | SQL **tool calling** (live metrics + trust) and **RAG** (policies/KPIs), gated by evals + observability. |
| When would we fine-tune? | Only for **stable behavior** — tool selection, house format, vocabulary, cost — never for facts. |
| Are we ready? | Yes — JSONL format defined, training data sourced from eval-passing traces, eval gate reused. Not switched on. |
