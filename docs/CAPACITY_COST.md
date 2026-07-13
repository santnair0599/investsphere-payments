# Capacity, Rate Limits & Cost — Azure OpenAI operations

The three operational questions for the GenAI plane, and how each is answered in this project.

| Question | Answer |
|---|---|
| **How much capacity do I have?** | Quota — TPM on the model deployment (§1) |
| **What happens when I exceed it?** | Rate limits — 429 + client backoff + APIM (§2) |
| **What is this costing me?** | Cost — per-run token accounting in `ai_control.agent_runs` (§3) |

---

## 1. Quota — how much capacity

Azure OpenAI capacity is **quota measured in tokens-per-minute (TPM)** (and a derived
requests-per-minute), assigned **per model, per region, per subscription**, and allocated to a
**deployment**.

- **Where it's set:** the deployment's `capacity` in `infra/azure/main.bicep`:
  ```bicep
  resource chat 'Microsoft.CognitiveServices/accounts/deployments@...' = {
    name: chatDeployment                         // gpt-4o
    sku: { name: 'Standard', capacity: 20 }      // 20 = 20,000 TPM
  }
  ```
  `capacity: 20` ≈ **20K TPM**. Raise it (subject to your regional quota) to scale; the value is a
  parameter so each environment can differ.
- **Deployment types (capacity models):**
  | Type | Capacity model | Use |
  |---|---|---|
  | **Standard** (regional) | Pay-as-you-go TPM in one region | dev/test, low volume |
  | **Global Standard** | Higher default TPM, global routing | most production PAYG |
  | **Provisioned (PTU)** | **Reserved** throughput, flat rate | predictable/high volume, latency SLAs |
- **Quota vs deployment:** subscription **quota** is the ceiling; a **deployment** draws from it.
  You can split quota across deployments (e.g. a cheap `gpt-4o-mini` for routing + `gpt-4o` for
  answers).
- **Requesting more:** regional quota increases go through Azure support / the Foundry quota page.
  For guaranteed capacity, move the hot path to **PTU**.

**Rule of thumb:** size TPM to *peak concurrent tokens/min* (prompt + completion) across all
callers, with headroom; if you can't tolerate throttling, buy PTU.

---

## 2. Rate limits — what happens when you exceed capacity

Exceed the deployment's TPM/RPM and Azure OpenAI returns **HTTP 429 (Too Many Requests)** with a
**`Retry-After`** header. This project handles it at three layers:

1. **SDK retry + backoff (implemented).** The Azure OpenAI client is created with
   `max_retries` (`ai/app/agent.py :: _client()`), so the SDK **automatically retries 429 and 5xx
   with exponential backoff, honouring `Retry-After`** — transient throttling is invisible to the
   caller.
   ```python
   AzureOpenAI(..., max_retries=int(os.environ.get("AZURE_OPENAI_MAX_RETRIES", "4")),
                    timeout=float(os.environ.get("AZURE_OPENAI_TIMEOUT", "30")))
   ```
2. **Graceful degradation (implemented).** If throttling persists past the retry budget, the agent
   does **not** crash — `_capacity_degrade()` returns a clear "temporarily at capacity, please
   retry" message and the run is traced with `safety_status = DEGRADED_CAPACITY` so it's visible in
   `ai_control.agent_runs`.
3. **Gateway enforcement (provisioned, opt-in).** **Azure API Management** is now in `main.bicep`
   as the Azure OpenAI gateway (`enableApim=true`). It provides proactive control: per-caller
   **`rate-limit-by-key`** (RPM), **`azure-openai-token-limit`** (per-key TPM + token metering),
   **`azure-openai-emit-token-metric`** → App Insights (cost attribution), and a **backend pool +
   `retry`** for **multi-deployment fallback** (set `secondaryOpenAiEndpoint` for a second region).
   APIM authenticates to Azure OpenAI keylessly via its managed identity; when enabled, the agent's
   `AZURE_OPENAI_ENDPOINT` routes through the gateway. Developer SKU provisions in ~30–45 min.

Also relevant: the **eval gate** enforces an `avg_latency_ms` threshold, so a change that pushes the
agent toward throttling/slowness fails CI before it ships.

**Config knobs:** `AZURE_OPENAI_MAX_RETRIES` (default 4), `AZURE_OPENAI_TIMEOUT` (default 30s).

---

## 3. Cost — what is this costing me

Cost is **token-based**: `input_tokens × input_rate + output_tokens × output_rate`, per model. This
project makes it a **query, not a guess**:

- **Per-run accounting (implemented).** `ai/app/agent.py` accumulates `response.usage`
  (`prompt_tokens` / `completion_tokens`) across every turn of the tool-calling loop, and
  `ai/observability/pricing.py` converts them to USD. Every run writes
  `model, tokens_in, tokens_out, estimated_cost` to **`ai_control.agent_runs`**.
- **Pricing table:** `ai/observability/pricing.py` — USD per 1M tokens per model (matches the
  longest deployment-name prefix, so `gpt-4o-mini` ≠ `gpt-4o`). Update to your region / negotiated /
  PTU-amortised rates.

### Answering the question in SQL
```sql
-- What did the agent cost today, and per user/desk?
SELECT user_id,
       COUNT(*)                         AS runs,
       SUM(tokens_in)  AS tokens_in,
       SUM(tokens_out) AS tokens_out,
       ROUND(SUM(estimated_cost), 2)    AS usd
FROM investsphere_prod.ai_control.agent_runs
WHERE created_at >= current_date()
GROUP BY user_id
ORDER BY usd DESC;

-- Cost per model + average latency (capacity health)
SELECT model, COUNT(*) runs, ROUND(SUM(estimated_cost),2) usd,
       ROUND(AVG(latency_ms)) avg_ms
FROM investsphere_prod.ai_control.agent_runs
GROUP BY model;
```

### Budgets & attribution
- **Per-user/desk attribution** — via `user_id` in `agent_runs` (and, with APIM, per subscription
  key for external metering/chargeback).
- **Budget alerting** — mirror the data-plane pattern: a Databricks SQL Alert / Azure Monitor rule
  on the `agent_runs` daily cost rollup (the same shape as the Databricks
  `cost_threshold_breach` alert in `docs/MONITORING.md`). Azure **Cost Management** budgets on the
  OpenAI resource cover the infra-billing side.

### Cost-control levers
- **Model routing (implemented, feature-flagged)** — `ai/app/model_router.py`, on via
  `MODEL_ROUTER_ENABLED=true`. Routes **simple** questions (policy lookups, ops-trust /
  pipeline-status / DQ) and the **tool-selection turns** to a cheap **fast** deployment
  (`AZURE_OPENAI_DEPLOYMENT_FAST`, e.g. `gpt-4o-mini`), and **complex** business-risk synthesis /
  cross-domain recommendations / final answer to a **reasoning** deployment
  (`AZURE_OPENAI_DEPLOYMENT_REASONING`). Pattern = **cheap-gather → expensive-synthesize**: the tool
  loop runs on the fast model; complex questions get one reasoning call for the final recommendation.
  Both runtimes; per-model cost + `model` + `routing_reason` land in `ai_control.agent_runs`.
  **Safe default:** OFF → the single `AZURE_OPENAI_DEPLOYMENT` (unchanged); fast unset → falls back.
  - *Managed alternative:* point `AZURE_OPENAI_DEPLOYMENT` at an **Azure AI Foundry Model Router**
    deployment (Azure auto-selects the model per request) — near-zero code, but a live Azure
    dependency, so the in-repo heuristic router stays the default for local/CI.
- **Prompt size** — the system prompt is the per-call tax; moving stable instructions into a
  fine-tuned model (see `docs/FINE_TUNING_STRATEGY.md`) trades training cost for lower per-call tokens.
- **Bounded loop** — `MAX_TOOL_TURNS` caps tokens per question.
- **Live groundedness monitoring is SAMPLED, not 100%** — `LIVE_GROUNDEDNESS_ENABLED` +
  `LIVE_GROUNDEDNESS_SAMPLE_RATE` (default 0.10). Each sampled answer costs one extra LLM-judge
  call (Azure AI Evaluation), so 10% sampling ≈ +10% of one judge call per query on average — a
  deliberate cost/coverage trade-off. Set the rate to 0 (or the flag off) to remove the cost;
  raise it for tighter monitoring. Results → `ai_control.agent_runs` (see
  `docs/EVALUATION_OBSERVABILITY.md` §5).
- **Caching / dedup** — repeated identical questions can be served from a short-TTL cache (future).
- **PTU** — at high, steady volume a provisioned deployment is cheaper per token than PAYG and
  removes throttling.

---

## Summary — what's implemented vs. what's next

| Area | Implemented | Next (optional) |
|---|---|---|
| Quota | deployment `capacity` (TPM) parameter in Bicep, documented | PTU deployment for the hot path |
| Rate limits | SDK `max_retries` + backoff + graceful degrade + trace status; eval-gate latency threshold; **APIM `rate-limit-by-key` + `azure-openai-token-limit` + backend-pool fallback (opt-in `enableApim`)** | second-region `secondaryOpenAiEndpoint`; PTU |
| Cost | per-run `tokens_in/out` + `estimated_cost` + `model` in `ai_control.agent_runs`; pricing table; SQL rollups; **APIM `azure-openai-emit-token-metric` → App Insights** | budget alert rule; per-key chargeback via APIM |

Config: `AZURE_OPENAI_MAX_RETRIES`, `AZURE_OPENAI_TIMEOUT`, deployment `capacity` (Bicep),
rates in `ai/observability/pricing.py`.
