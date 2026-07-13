# RUNBOOK — InvestSphere Enterprise Lakehouse + GenAI Agent

Two independent planes you can test separately:

- **Plane A — Lakehouse** (Databricks): synthetic Bronze → Silver → Gold → verify. **No Azure/Foundry creds needed.**
- **Plane B — GenAI agent** (Azure): RAG index → agent → eval gate. **Needs Azure creds.**

Everything is code-complete offline (`dbt parse` clean, all modules import, guardrails verified). Below is exactly what to run.

---

## Plane A — Lakehouse demo (Databricks only)

**Fixed values — use these exactly:** `catalog = investsphere_dev`, `run_id = demo_run_001`.
(The Silver conformers filter Bronze by `run_id`; a different value → Silver reads 0 rows.)

```bash
# 0. Authenticate (from your machine)
databricks auth login --host https://<your-workspace>.azuredatabricks.net

# 1. Validate the bundle (should pass once authenticated)
databricks bundle validate -t dev
```

**In the workspace (cluster with Spark + this repo synced as a Databricks Repo):**

```
2. Run notebook  notebooks/00_generate_synthetic_enterprise_data.py
     widgets: catalog=investsphere_dev  run_id=demo_run_001
     -> writes 19 Bronze tables + silver_cdc.customer_scd2 + control tables
        (with intentional # BAD: rows for the DQ/quarantine path)

3. Run notebook  notebooks/01_run_silver_conformers.py
     widgets: catalog=investsphere_dev  run_id=demo_run_001
     -> runs the 5 domain conformers (parse/DQ/quarantine/dedup/MERGE)
        (silver_customer_scd2 is skipped — already seeded by notebook 00)
```

**dbt Gold** (from a terminal with the Databricks dbt profile, or a dbt task):

```bash
cd dbt
dbt build --vars '{catalog: investsphere_dev}'
dbt test  --vars '{catalog: investsphere_dev}'
```

```
4. Run notebook  notebooks/02_verify_enterprise_demo.py
     widget: catalog=investsphere_dev
     -> runs the SQL checks + a one-glance PASS/FAIL assertion
```

### ⚠️ Do NOT run these for the synthetic demo
`bronze_payments_file`, `bronze_jdbc`, `bronze_customer_cdc`, `bronze_rest_api`,
`bronze_sftp`, `bronze_salesforce` — these ingest from **real** Oracle/SQL Server/
Salesforce/SFTP/REST/CDC sources and need credentials. For this demo, **notebook 00 IS
the Bronze layer.** Running the full `daily_e2e` job from the top would try real
ingestion and overwrite the synthetic Bronze.
(The full `init → bronze → gate → silver → gate → dbt` DAG is for the real-source path,
which is a separate exercise once source credentials are in Key Vault.)

### Acceptance checks (the verify notebook automates these)
```sql
SELECT COUNT(*) FROM investsphere_dev.silver_quarantine.failed_records;               -- > 0
SELECT * FROM investsphere_dev.gold_ops_trust.mart_business_recommendation_trust;     -- trust_level + confidence_score
SELECT * FROM investsphere_dev.gold_realestate.mart_property_underperformance;        -- rows + risk_reasons
SELECT * FROM investsphere_dev.gold_hospitality.mart_hotel_revenue_risk;
SELECT * FROM investsphere_dev.gold_entertainment.mart_venue_conversion_risk;
SELECT * FROM investsphere_dev.gold_investment.mart_investment_risk;
SELECT * FROM investsphere_dev.gold_customer.mart_declining_customer_segments;
```
**Good result:** quarantine has rows · all 5 marts populated · `risk_reasons` filled ·
`gold_ops_trust` shows SUCCESS + PARTIAL · `confidence_score` present · `dbt test` green.

---

## Plane B — GenAI agent (needs Azure credentials)

Do this only after Plane A is clean.

```bash
# 1. Provision Azure infra (Foundry hub/project + its Storage, OpenAI, AI Search,
#    Container App + ACR pull, Key Vault, App Insights)
az deployment group create -g <rg> --template-file infra/azure/main.bicep \
   --parameters agentImage=<acr>.azurecr.io/investsphere-agent:latest \
                databricksToken=<ro-sql-warehouse-token> \
                databricksHost=<host> databricksHttpPath=<http_path>
#    (add enableApim=true secondaryOpenAiEndpoint=<url> to provision the APIM gateway)

# 1b. Grant the agent's managed identity AcrPull so the Container App can pull the image.
#     The deployment outputs `agentIdentityPrincipalId`.
PRINCIPAL=$(az deployment group show -g <rg> -n main --query properties.outputs.agentIdentityPrincipalId.value -o tsv)
az role assignment create --assignee "$PRINCIPAL" --role AcrPull \
   --scope $(az acr show -n <acr> --query id -o tsv)

# 2. Env for the tools/agent (locally or in the Container App — normally Key Vault refs)
export AZURE_OPENAI_ENDPOINT=...      AZURE_OPENAI_API_KEY=...
export AZURE_OPENAI_DEPLOYMENT=gpt-4o AZURE_OPENAI_EMBED_DEPLOYMENT=text-embedding-3-small
export AZURE_SEARCH_ENDPOINT=...      AZURE_SEARCH_KEY=...      AZURE_SEARCH_INDEX=investsphere-policies
export DATABRICKS_HOST=...  DATABRICKS_HTTP_PATH=...  DATABRICKS_TOKEN=...  DATABRICKS_CATALOG=investsphere_dev

# 3. Create the observability + rollup objects (run once, on the SQL Warehouse; sub {{catalog}})
#    ai/observability/ddl.sql                 -- ai_control.agent_runs / agent_tool_calls / evaluations / prompt_versions
#    ai/observability/hallucination_rollup.sql -- live hallucination-rate views (after enabling live groundedness)

# 4. Build the RAG index from the policy corpus
pip install -r ai/requirements.txt
python -m ai.rag.index_policies                 # chunks + embeds ai/rag/policies/*.md -> Azure AI Search

# 5. (Foundry twin) generate the OpenAPI tool spec + provision the Foundry agent
python -m ai.foundry.build_openapi_tools         # writes ai/foundry/openapi_tools.json (no creds)
python -m ai.foundry.provision_agent --dry-run   # prints the resolved agent (no creds)
python -m ai.foundry.provision_agent             # creates the Foundry agent (needs creds)

# 6. Run the agent API locally (or it runs in the Container App)
uvicorn ai.app.main:app --port 8000
curl localhost:8000/health
curl localhost:8000/tools
curl -X POST localhost:8000/ask -H 'content-type: application/json' \
     -d '{"question":"Which hotels have revenue risk this week?"}'

# 7. Eval gate (blocks a bad deploy)
python -m ai.eval.run_evals --dry-run            # offline: validates dataset (no creds)
python -m ai.eval.run_evals                      # live: scores 30 Qs, exits non-zero on threshold breach
```

CI wires steps 4–7 in `.github/workflows/ai-deploy.yml` (eval gate on PRs, deploy on main).

### Optional Azure features — feature flags (all default OFF; see `ai/.env.example`)
Turn on in the Container App env (or `.env`) once creds are in place. Nothing is required for the
core agent; each is additive and safe-by-default:

| Flag | Turns on | Needs |
|---|---|---|
| `AGENT_RUNTIME=langgraph` | LangGraph StateGraph runtime (else raw SDK loop) | — |
| `MODEL_ROUTER_ENABLED=true` | fast/reasoning model routing | `AZURE_OPENAI_DEPLOYMENT_FAST` + `_REASONING` |
| `EVAL_GATE_ENABLED=true` | Azure AI Evaluation SDK evaluators in the gate | Azure OpenAI (judge) |
| `TRACING_ENABLED=true` | OpenTelemetry → App Insights spans | `APPLICATIONINSIGHTS_CONNECTION_STRING` |
| `PROMPT_SHIELDS_ENABLED=true` | live Content Safety Prompt Shields | `AZURE_CONTENT_SAFETY_ENDPOINT` |
| `LIVE_GROUNDEDNESS_ENABLED=true` | sampled live hallucination rate | Azure OpenAI (judge); `..._SAMPLE_RATE` |
| `SENTIMENT_ENRICHMENT_ENABLED=true` | Azure AI Language sentiment (else lexicon) | `AZURE_LANGUAGE_ENDPOINT` |
| `enableApim=true` (Bicep) | APIM gateway: rate-limit-by-key + token metering + fallback | APIM (Developer SKU) |

---

## Troubleshooting

### 1. Expired / invalid Databricks auth (`403 Invalid Authorization` on `bundle validate`)
The bundle parses fine; the 403 is only the workspace identity check. Re-auth:
```bash
databricks auth login --host https://<workspace>.azuredatabricks.net
databricks auth describe          # confirm the token/profile
databricks bundle validate -t dev
```
If using a profile: `databricks bundle validate -t dev -p <profile>`.

### 2. Empty Gold marts
The domain marts are anchored on the **latest date present in the data** (not the wall
clock), so synthetic data dated in the past still populates them. If a mart is still empty:
- **Silver is empty** → the `run_id` didn't match. Re-run notebook 01 with `run_id=demo_run_001`
  (the same value used in notebook 00). Confirm: `SELECT COUNT(*) FROM investsphere_dev.silver_realestate.property_clean;`
- **Silver has rows but mart is empty** → the conformers didn't run before `dbt build`, or a
  cross-domain dependency (customer sentiment reads hospitality) is missing — run ALL 5 conformers.
- Legacy check (if you reverted the date-anchor fix): marts using `current_date()` need the
  synthetic fact dates within ~30 days of the cluster date — either bump the dates in notebook 00
  to `date_sub(current_date(), N)` or widen the mart window.

### 3. Missing Azure credentials
The `ai/` modules import fine without any Azure/OpenAI/Databricks SDK (all lazy). What
needs creds: `index_policies` (Azure OpenAI + Search), the live agent `/ask` (all three),
`provision_agent` (Foundry), live `run_evals`. Use `--dry-run` variants and
`python -m ai.eval.run_evals --dry-run` to exercise wiring **without** creds. Store secrets
in Key Vault; the Container App reads them via managed identity (see `infra/azure/main.bicep`).

### 4. Azure AI Search index setup
`python -m ai.rag.index_policies` **creates** the index (`investsphere-policies`) with the
vector + semantic config, then chunks/embeds `ai/rag/policies/*.md` and uploads. Requirements:
- `AZURE_SEARCH_ENDPOINT`, `AZURE_SEARCH_KEY`, `AZURE_SEARCH_INDEX` set
- an embedding deployment (`AZURE_OPENAI_EMBED_DEPLOYMENT`, default `text-embedding-3-small`)
- the Search service SKU must support **semantic** ranking (Basic+; the Bicep sets `semanticSearch: standard`)
If retrieval returns nothing: confirm the index exists and has documents
(`az search ...` or the portal), and that `semantic_configuration_name="default"` matches
the config created by the indexer.

---

## Where things live
- Lakehouse: `notebooks/00-02`, `src/payments_platform/databricks/silver_*.py`, `dbt/models/gold_*`
- GenAI: `ai/` (tools, app, rag, guardrails, observability, eval, foundry), `infra/azure/main.bicep`, `.github/workflows/ai-deploy.yml`
- Design/story: `docs/ENTERPRISE_PIVOT.md`, `docs/GENAI_INTERVIEW_STORY.md`, `docs/EXECUTION_STATUS.md`
