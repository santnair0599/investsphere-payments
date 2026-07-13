# Execution Status — Runtime Consistency Validation

Evidence-based status. Last validated **2026-07-08** (offline, no live workspace/creds).
This is validation + evidence only — no features, refactoring, renaming, or architecture changes.

## ✅ Verified offline (no cluster, no cloud credentials)

| # | Check | Result |
|---|---|---|
| 1 | `dbt parse --no-partial-parse` | **exit 0** (only style deprecations; no errors) |
| 2 | `databricks bundle validate -t dev` | **structure valid** — resolves `Name: investsphere_payments`, `Target: dev`; the only error is `403 Invalid Authorization` (workspace identity check → needs `databricks auth login`) |
| 3 | `daily_e2e` task count in `databricks.yml` | **22 tasks — CONFIRMED** (the deployed truth) |
| 4 | Every `task_key` resolves | **all 22 resolve** — 18 `spark_python_task` → `pipelines/dag_task.py` (exists), 2 `dbt_task` (dbt commands), 2 `condition_task` (no file) |
| 5 | 5 domain Silver conformers wired **before** the Silver DQ gate | **PASS** — `silver_realestate/hospitality/entertainment/investment/customer` each `depends_on: bronze_gate_check` (outcome `"true"`) |
| 6 | Silver DQ gate depends on all required Silver tasks | **PASS** — `silver_dq_gate.depends_on` = `[silver_customer_scd2, silver_realestate, silver_hospitality, silver_entertainment, silver_investment, silver_customer]` |
| 7 | Gold models read domain Silver, **not** payments tables | **PASS** — zero references to `silver_clean` / `payment_clean` / `fact_payments` / `daily_payment_summary`; each `gold_<domain>` reads `source('silver_<domain>', …)`; `gold_ops_trust` reads `silver_control` + `silver_quarantine` |
| — | `pipelines/dag_task.py` dispatches all 6 Silver tasks | **PASS** — `silver_realestate/hospitality/entertainment/investment/customer/customer_scd2` all handled |
| — | dbt model count | **28 `gold_*` models** + 3 shared/staging (`dim_customer`, `dim_customer_history`, `stg_customer`) = **31 total** |

### Task-graph (verified in `databricks.yml`)
```
init_run
 → [bronze_payments_file, bronze_jdbc, bronze_customer_cdc, bronze_rest_api, bronze_sftp, bronze_salesforce]
   → bronze_validation_gate → bronze_gate_check (condition: gate_passed == "true")
     → [silver_customer_scd2, silver_realestate, silver_hospitality, silver_entertainment,
        silver_investment, silver_customer]
       → silver_dq_gate → silver_dq_check (condition)
         → dbt_build → dbt_test → governance_validation → publish_notify → write_status
```

### Docs consistency (grep evidence)
| Term | Status |
|---|---|
| **22-task DAG** | consistent — **0** stale "15-task" remain (docs + README corrected to 22) |
| **6 source domains** | present in 7 docs |
| **5 domain Silver conformers** | dispatched in code (6/6) + described in docs (`silver_realestate` etc.) |
| **28 dbt models** | count verified from repo (28 `gold_*`); not stated as a number in prose docs |
| **`gold_ops_trust`** | referenced in 13 docs |
| **Azure GenAI plane** | referenced in 17 docs (Foundry / LangGraph / decision agent) |

## 🔷 Requires a live Databricks workspace
- Actually **running** the pipeline (offline we prove it *compiles + wires*; only a cluster proves it *runs*):
  1. `databricks auth login` (clears the 403), then `databricks bundle validate/deploy`.
  2. `notebooks/00_generate_synthetic_enterprise_data.py` (catalog `investsphere_dev`, run_id `demo_run_001`).
  3. `notebooks/01_run_silver_conformers.py` (same `run_id`) — the 5 domain conformers.
  4. `dbt build && dbt test` (`--vars '{catalog: investsphere_dev}'`).
  5. `notebooks/02_verify_enterprise_demo.py` — populated marts + trust score + quarantine.
- Populating `silver_quarantine.failed_records` (written by the conformers at runtime) and the `gold_*` marts (dbt).
- Unity Catalog grants + masked views (`governance/sql/`), `ai/observability/ddl.sql`, `hallucination_rollup.sql`.

## 🔶 Requires live Azure credentials
- The GenAI agent end-to-end (`/ask`, `/recommend`): **Azure OpenAI** + **Azure AI Search** (+ `python -m ai.rag.index_policies`) + a read-only **Databricks SQL Warehouse** token.
- Bicep deploy of the Azure plane (`infra/azure/main.bicep`): Foundry hub (+ its Storage) / OpenAI / AI Search / Container Apps / ACR pull / Key Vault / App Insights.
- The live eval gate (`python -m ai.eval.run_evals`) and all feature-flagged Azure features (see the RUNBOOK feature-flag table): model router, tracing, Prompt Shields, live groundedness, sentiment.

## ⚠️ Known limitations (evidence-recorded, not changed this round)
- **`databricks bundle validate` returns 403** offline — expected (expired/absent workspace token); structure is valid. Re-auth with `databricks auth login`.
- **README body** (below the enterprise banner) still describes the **payments-era foundation** and its "What's implemented" table + ASCII/Mermaid diagrams reference the **deleted** dbt models `fact_payments` / `daily_payment_summary` and payments source labels. Framed by the banner as "the data-platform foundation," but these are stale references (not corrected — out of the validation-only scope).
- **`src/payments_platform/orchestration/dag.py`** (the policy-as-code reference DAG) was **not** updated with the 5 domain conformers, so it enumerates ~15 tasks vs the bundle's **22** — the "mirrored 1:1" claim is now imprecise (code gap, not a doc gap).
- **`bi/` generator code** still emits payments-carryover view names (e.g. `v_payments_daily_bi`) while serving the enterprise marts.
- Retained legacy **payments-practice file path** (`silver_payments.py`, `bronze.payments_file`, `silver_clean.payment_clean`, `seeds/payments/`) is intentionally kept as one retained domain — real code, not stale.
- Internal identifiers **`payments_platform`** (package) and **`investsphere_payments`** (bundle + dbt project + job + secret scope) are intentional and unchanged.
- No load/scale testing; single-region by default; APIM is opt-in (`enableApim=true`, Developer SKU ~30–45 min).

## Summary
The lakehouse pipeline is **structurally consistent and deployment-ready**: dbt parses, the 22-task bundle validates (auth aside), every task resolves, the 5 domain conformers are wired between the bronze gate and the Silver DQ gate, and Gold reads only the enterprise domain Silver sources. Remaining items are the known limitations above — chiefly the README-body foundation description and the `orchestration/dag.py` / `bi/` code carryovers, all recorded here, none blocking a deploy.
