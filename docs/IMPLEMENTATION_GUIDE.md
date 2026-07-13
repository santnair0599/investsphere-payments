# Implementation Guide — from zero to running on Azure + Databricks

A from-scratch guide to stand up this project on **real** infrastructure. It covers the setup the
`RUNBOOK.md` assumes already exists (accounts, workspace, catalog, ACR, auth), then hands off to the
RUNBOOK for the run commands. Two planes; do **Plane A first** (it needs no Azure AI and proves the
whole lakehouse).

> **Recommended low-cost path (this guide's default):** Databricks **Free Edition** (serverless, $0)
> for the data plane + a **new Azure account** ($200 free credit / 30 days) for the GenAI plane. Paid
> **Azure Databricks** notes are called out where they differ.

---

## Phase 0 — Prerequisites & decisions

### Install (local machine)
```bash
# Azure CLI, Databricks CLI, Python 3.11, Docker, Git
az --version
databricks --version
python --version        # 3.11
docker --version
```

### Two decisions that bite later if wrong
1. **Azure OpenAI region** — choose one with **GPT-4o + text-embedding-3-small** quota
   (East US / Sweden Central are safe). Check/raise quota **before** deploying (Azure AI Foundry →
   Quotas). See `docs/CAPACITY_COST.md` §1.
2. **Your deploy identity needs `Owner` or `User Access Administrator`** on the resource group — the
   Foundry hub + Container App create role assignments at deploy time.

### Cost expectation
Tear-down-style demo: **~$15–50** (or near-$0 with Free Edition + free credit). Full stack left
running 24/7: ~$180–350/mo. Details + teardown in `docs/CAPACITY_COST.md`.

---

## Phase 1 — Databricks data plane (no Azure AI needed)

**Goal:** synthetic Bronze → Silver conformers → dbt Gold → verified marts + trust score.

### 1.1 Workspace + catalog
- **Free Edition:** sign up at the Databricks Free Edition portal → you get a serverless workspace.
  Create the catalog + schemas manually (SQL editor):
  ```sql
  CREATE CATALOG IF NOT EXISTS investsphere_dev;
  ```
  (The generator notebook creates the `bronze`/`silver_*`/`gold_*` schemas it needs; dbt creates the
  `gold_*` schemas on write.)
- **Azure Databricks (paid):** provision via `infra/terraform` (`envs/dev`) — RG + ADLS Gen2 + Unity
  Catalog + SQL Warehouse + groups. See `docs/TERRAFORM.md`.

### 1.2 Get the repo into the workspace
Databricks **Repos** → add repo (your git remote), or upload the folder. You need `notebooks/`,
`src/payments_platform/`, `dbt/`, `databricks.yml`.

### 1.3 Authenticate + validate the bundle
```bash
databricks auth login --host https://<your-workspace>.azuredatabricks.net
databricks bundle validate -t dev        # should resolve Name: investsphere_payments
databricks bundle deploy -t dev          # deploys jobs (optional; you can run notebooks directly)
```

### 1.4 Run the demo path (in the workspace)
```
notebook 00_generate_synthetic_enterprise_data   (widgets: catalog=investsphere_dev, run_id=demo_run_001)
notebook 01_run_silver_conformers                (SAME run_id=demo_run_001)
dbt build --vars '{catalog: investsphere_dev}'   (from a terminal / dbt task)
dbt test  --vars '{catalog: investsphere_dev}'
notebook 02_verify_enterprise_demo               (catalog=investsphere_dev)
```
> **Critical:** `run_id=demo_run_001` in BOTH notebooks — the conformers filter Bronze by it.
> **Do NOT** run the real `bronze_*` ingest tasks — notebook 00 *is* your Bronze for the demo.

### 1.5 Success criteria
- `silver_quarantine.failed_records` has rows (the intentional bad records)
- all six `gold_*` `mart_*` tables populated with `risk_reasons`
- `gold_ops_trust.mart_business_recommendation_trust` returns a `trust_level` + `confidence_score`
- `dbt test` green

**At this point the entire lakehouse works with zero Azure AI spend.** See `RUNBOOK.md` → Plane A.

---

## Phase 2 — Azure GenAI plane

**Goal:** the agent answers business questions over the governed marts.

### 2.1 Azure basics
```bash
az login
az group create -n investsphere-rg -l eastus
```

### 2.2 Azure OpenAI + model deployments
Create an Azure OpenAI resource; deploy **`gpt-4o`** and **`text-embedding-3-small`** (the Bicep can
do this, or do it in the Foundry portal first to confirm quota).

### 2.3 Build + push the agent image (ACR)
```bash
az acr create -g investsphere-rg -n <acrname> --sku Basic
az acr login -n <acrname>
docker build -t <acrname>.azurecr.io/investsphere-agent:v1 -f ai/Dockerfile .
docker push <acrname>.azurecr.io/investsphere-agent:v1
```

### 2.4 Provision the Azure plane (Bicep)
```bash
az deployment group create -g investsphere-rg --template-file infra/azure/main.bicep \
  --parameters agentImage=<acrname>.azurecr.io/investsphere-agent:v1 \
               databricksHost=<host> databricksHttpPath=<http_path>
# No databricksToken — the Container App's system-assigned managed identity mints an
# Entra token for Databricks at runtime (ai/tools/databricks_client._aad_token). Add
# that MI as a Databricks workspace SP with gold_* read grants. See CICD_SETUP.md.
```
This creates: Foundry hub **+ its storage**, OpenAI, AI Search, Container Apps, Key Vault, App
Insights, managed identity, ACR-pull wiring.

### 2.5 Grant the agent identity AcrPull (one-time)
```bash
PRINCIPAL=$(az deployment group show -g investsphere-rg -n main \
  --query properties.outputs.agentIdentityPrincipalId.value -o tsv)
az role assignment create --assignee "$PRINCIPAL" --role AcrPull \
  --scope $(az acr show -n <acrname> --query id -o tsv)
```

### 2.6 Observability tables + RAG index
```bash
# on the SQL Warehouse (substitute {{catalog}}):
#   ai/observability/ddl.sql   +   ai/observability/hallucination_rollup.sql
pip install -r ai/requirements.txt
python -m ai.rag.index_policies          # builds the Azure AI Search index from ai/rag/policies/*.md
```

### 2.7 Test the agent
```bash
FQDN=$(az containerapp show -g investsphere-rg -n investsphere-ai-agent \
  --query properties.configuration.ingress.fqdn -o tsv)
curl https://$FQDN/health
curl https://$FQDN/tools
curl -X POST https://$FQDN/ask -H 'content-type: application/json' \
  -d '{"question":"Which hotels have revenue risk this week?"}'
```

---

## Phase 3 — Turn on Azure-native features (optional, one at a time)
All default **OFF**; enable in the Container App env after the core agent works. See the RUNBOOK
feature-flag table:
`AGENT_RUNTIME=langgraph` · `EVAL_GATE_ENABLED` · `TRACING_ENABLED` · `PROMPT_SHIELDS_ENABLED` ·
`MODEL_ROUTER_ENABLED` · `LIVE_GROUNDEDNESS_ENABLED` · `SENTIMENT_ENRICHMENT_ENABLED` ·
`enableApim=true` (Bicep; ~30–45 min to provision).

---

## Troubleshooting (the ones that actually bite)
| Symptom | Fix |
|---|---|
| `403 Invalid Authorization` on bundle validate | `databricks auth login` — the token expired |
| Silver reads 0 rows | `run_id` mismatch — use `demo_run_001` in notebooks 00 **and** 01 |
| Gold mart empty but Silver has rows | run ALL 5 conformers before `dbt build` (marts anchor on max data date, so date isn't the issue) |
| Container App won't pull image | grant the agent identity **AcrPull** (Phase 2.5) |
| Foundry hub deploy fails | deploy principal needs Owner/User Access Admin; region has OpenAI quota |
| Agent 401 to OpenAI | check Key Vault secrets + managed identity (the app reads secrets via UAMI) |

## Order of operations (one line)
```
Phase 0 (accounts+quota) → Phase 1 (Databricks: catalog → repo → auth → nb00 → nb01 → dbt → nb02)
  → Phase 2 (Azure: RG → OpenAI → ACR → Bicep → AcrPull → DDL → index → test /ask)
  → Phase 3 (enable flags one at a time) → teardown when idle (az group delete)
```

See also: `RUNBOOK.md` (commands), `docs/EXECUTION_STATUS.md` (verified vs needs-creds),
`docs/CAPACITY_COST.md` (quota/cost/teardown), `docs/ENTERPRISE_PIVOT.md` (the design).
