# Changelog

All notable changes to InvestSphere Payments are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/); the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [2.1.0] — GenAI hardening, model selection & production delivery
### Added (AI plane — enhancements 6–12, all feature-flagged w/ offline fallbacks)
- **Foundry IQ agentic-retrieval benchmark** (`ai/benchmarks/`) — recall/precision/MRR/nDCG
  + agentic-vs-baseline lift.
- **Document Intelligence reconciliation** (`ai/docintel/`) — OCR extract → reconcile
  against `gold.fact_payments` (MATCH/MISMATCH/MISSING).
- **Production failure + load tests** (`ai/tests/`) — locust SLO gate + chaos/resilience
  tests (mocked client → real degradation paths).
- **Responsible-AI red-team suite** (`ai/redteam/`) — ASR per category; blocks CI on breach.
- **Arabic bilingual retrieval + parity** (`ai/i18n/`, `ai/rag/policies/ar/`) — AR/EN
  retrieval + answer parity gate.
- **Streaming HITL approval UI** (`ai/ui/`) — SSE answers; sensitive actions pause for approval.
- **Microsoft Teams publishing** (`ai/integrations/teams/`) — Adaptive Cards for
  recommendations / alerts / answers.
### Added (model selection)
- `docs/MODEL_SELECTION.md` — evaluation criteria, benchmark harness, and the **two-tier
  GPT-4o + GPT-4o-mini** decision via the model router; reproduce with `ai.eval.run_evals`
  + the retrieval/red-team/parity suites.
### Changed (CI/CD → production-grade delivery)
- **Offline AI quality gate** (`ai/ci/run_quality_gate.py`, `quality_gates.yaml`) — required
  PR check: unit · chaos · red-team · Arabic · retrieval-lift · structured-output · authz;
  emits an evidence report.
- **Reusable deploy** (`ai-deploy.yml` `workflow_call`) with **build-once / promote-by-digest**;
  prod ships the **test-approved digest** (`deploy-prod needs deploy-test`).
- **Azure OIDC / workload identity federation** — no stored SP secret; **least-privilege
  custom deploy role** (not Contributor); **Databricks token → managed-identity Entra token**.
- **Blue-green + canary soak + auto-rollback**; deploy-evidence + smoke-result artifacts;
  **nightly live-Azure gate** + Teams failure alerts.
- `docs/CICD_SETUP.md`, `scripts/setup_azure_oidc.sh`, `scripts/setup_github_cicd.sh`,
  `ai/README_ENHANCEMENTS.md`.

## [2.0.0] — Diversified enterprise + GenAI decision agent
### Changed (data platform)
- **Pivoted the domain** from payments to a Dubai Holding-style diversified enterprise
  (real estate, hospitality, entertainment, investment, customer). The **six ingestion
  patterns and the entire engineering foundation are preserved** — only the business
  payloads/tables changed. See `docs/ENTERPRISE_PIVOT.md`.
- Repointed all 6 source configs (JDBC→Oracle PMS + SQL Server treasury, REST→booking
  platform + FX, Salesforce→CRM, SFTP→ticketing, Autoloader→campaign, CDC→guest master).
- Added 5 domain **Silver conformers** (`src/payments_platform/databricks/silver_*.py`),
  each mirroring the DQ/quarantine/dedup/MERGE pattern; wired into the bundle DAG.
- Replaced payments Gold with **6 Gold schemas** (`dbt/models/gold_*`): dims/facts +
  a `mart_*` risk surface per domain, plus `gold_ops_trust` (pipeline/DQ/freshness/trust).
  `dbt parse` clean across all 29 models.
### Added (Azure GenAI plane — `ai/`)
- **Enterprise Decision Agent**: LangGraph/FastAPI runtime + Azure AI Foundry config,
  Azure OpenAI, tool-calling over governed marts, Azure AI Search **RAG** (hybrid+semantic)
  over 8 policy/KPI docs.
- **Trust gate**, deterministic **guardrails** (PII/injection/approval), **30-question
  eval set** + CI **eval gate**, `ai_control.*` **observability** tables.
- **Bicep** infra (Foundry/OpenAI/Search/Container Apps/Key Vault/App Insights) +
  GitHub Actions AI deploy pipeline; UC read-only grants + masked customer view.

### Summary — full Databricks execution layer
The platform now runs **end to end for real on Spark**, not just as a tested reference.
All six Bronze sources (file/Auto Loader, customer CDC, JDBC, REST, SFTP, Salesforce),
the SCD2 Silver, the payments Silver, and both validation gates execute as PySpark under
`src/payments_platform/databricks/`, wired into the `daily_e2e` Lakeflow Job via
`pipelines/dag_task.py`; dbt Gold builds the full star (`dim_customer` /
`dim_customer_history` / `fact_payments` / `daily_payment_summary`). Ingestion is
watermark/checksum-tracked in `silver_control` (advanced only after a successful write),
the gates run actual checks and block promotion, and every gate + pipeline-run outcome is
persisted to `silver_control.{pipeline_run_audit,dq_results,table_load_status}` with a
Databricks SQL dashboard + alerts (`docs/GATE_MONITORING.md`). The cloud-agnostic data
logic in `payments_platform.*` is imported by the Spark jobs, so runtime and design can't
drift. Eleven validation notebooks (`notebooks/`) cover each stage. Deployment-focused:
no local pytest suite; validated in-workspace + via the credential-free smoke test and CI.

### Added — real Databricks execution layer (file path)
- `src/payments_platform/databricks/bronze_payments_autoloader.py` — **Auto Loader**
  Bronze ingestion for the payments file feed (`cloudFiles`, schema evolution,
  `_rescued_data`, checkpoint, `availableNow`), writing `bronze.payments_file`.
- `src/payments_platform/databricks/silver_payments.py` — Spark Silver: parse/cast
  → DQ → quarantine (`silver_quarantine.failed_records`) → dedup → **Delta MERGE**
  into `silver_clean.payment_clean`; reuses the tested DQ allowed-value sets.
- `pipelines/dag_task.py` dispatches `bronze_payments_file` / `silver_payments` to
  the real modules (other sources remain reference stubs).
- `databricks.yml` passes `--catalog` to the file task; dbt narrowed to the
  payments lineage with a `catalog` var; documented serverless wheel packaging.
- `dbt/` sources + models read/write the catalog from a `var('catalog')`.
- `docs/AZURE_IMPLEMENTATION.md` — step-by-step runbook for the file-based path.

### Added — customer CDC → SCD2 execution (real Spark)
- `src/payments_platform/databricks/bronze_customer_cdc.py` — **Auto Loader** JSON
  ingestion of Debezium events into `bronze.customer_cdc`, preserving before/after
  images + `operation_type` / `sequence_number` (source LSN) / `event_timestamp`;
  op codes reuse `bronze.cdc_ingest._OP_MAP`.
- `src/payments_platform/databricks/silver_customer_scd2.py` — **Delta SCD2 MERGE**
  into `silver_cdc.customer_scd2` (expire-and-insert, soft-delete, hash
  change-detect, sequence ordering; `effective_from/to`, `is_current`, `is_deleted`,
  `record_hash`, `sequence_number`). Events collapse to the final state per key per
  batch; history accumulates across batches.
- `pipelines/dag_task.py` dispatches `bronze_customer_cdc` / `silver_customer_scd2`
  to the real modules (removed their NOT-IMPLEMENTED branch); `databricks.yml` now
  passes `--catalog` to the CDC task.
- `notebooks/04_validate_cdc_scd2.py` — smoke queries for Bronze CDC events and the
  current/history/soft-deleted SCD2 rows.
- dbt Gold widened to the full graph: `databricks.yml` now runs `dbt build`/`dbt test`
  over all models, so `dim_customer` (current SCD2 rows), `dim_customer_history`
  (full history), and `gold_marts.daily_payment_summary` (fact ⋈ customer dimension)
  build off the real `silver_cdc.customer_scd2`.
- The `fact_payments.customer_id` relationships test now targets
  `dim_customer_history` (the full customer master) so a payment that predates a
  soft-delete still resolves; `dim_customer` continues to exclude soft-deleted
  customers.
- `notebooks/05_validate_gold_dims.py` — smoke queries: current dimension,
  soft-deletes excluded from `dim_customer` but flagged in history, and the daily
  payment summary.

### Added — JDBC incremental ingestion (real Spark)
- `src/payments_platform/databricks/bronze_jdbc.py` — metadata-driven **Spark JDBC**
  ingestion of Oracle / SQL Server tables into Bronze Delta. Full loads overwrite the
  snapshot; incremental loads push a `watermark_col > last_watermark` predicate down to
  the source DB and append; a first incremental run is a full seed. The watermark is
  persisted/advanced (forward-only) in `{catalog}.silver_control.ingestion_watermark`.
  Credentials come from the KV-backed secret scope (`<source_system>-jdbc-url/user/password`)
  via `dbutils.secrets`; reuses the reference `source_config` loader + `jdbc_ingest`
  audit constants.
  - Per-table read tuning: `fetchsize`, and optional bounded parallel read
    (`partition_column` / `lower_bound` / `upper_bound` / `num_partitions`) for large tables.
  - Dialect-aware watermark predicate builder (Oracle `TO_DATE`/`TO_TIMESTAMP`,
    SQL Server `CAST(... AS DATETIME2)`), pushed down to the source DB.
  - Watermark advances **only after a successful write**, derived from the written
    Bronze rows (never ahead of what landed); a failed write leaves the control row
    untouched so a retry re-reads from the last good watermark.
- `pipelines/dag_task.py` dispatches `bronze_jdbc` to the real module (removed its
  NOT-IMPLEMENTED branch); `databricks.yml` now passes `--catalog` to the task.
- `notebooks/06_validate_jdbc_bronze.py` — smoke queries for the Bronze JDBC tables
  and the ingestion watermark control table.

### Added — REST API incremental ingestion (real Spark)
- `src/payments_platform/databricks/bronze_rest_api.py` — metadata-driven REST
  ingestion into Bronze Delta (`requests` on the driver), driven by
  `seeds/rest/api_config.json`. Page-number **and** cursor pagination; incremental
  `updated_since = <watermark>`; retry/backoff on 429/5xx + network errors (honours
  `Retry-After`); raw payload stored as `_raw_response` alongside inferred columns.
  Audit columns `source_system` / `api_name` / `endpoint` / `run_id` /
  `ingestion_timestamp` / `source_extract_timestamp` / `record_hash`. Bearer token +
  base URL from the secret scope (`dbutils.secrets`); watermark persisted/advanced
  forward-only **after a successful write** in `silver_control.ingestion_watermark`
  (keyed by `api_name.endpoint`). Reuses `source_config` / `rest_ingest` constants /
  `config.secrets` / the JDBC watermark helpers.
- `pipelines/dag_task.py` dispatches `bronze_rest_api` to the real module (removed its
  NOT-IMPLEMENTED branch); `databricks.yml` now passes `--catalog` to the task.
- `notebooks/07_validate_rest_bronze.py` — smoke queries for the Bronze REST tables,
  the watermark control table, and the watermark-not-ahead invariant.

### Added — SFTP vendor-file ingestion (real Spark)
- `src/payments_platform/databricks/bronze_sftp.py` — ingests vendor files that
  landed in a UC Volume into Bronze Delta, driven by `seeds/sftp/file_config.json`.
  Lists the landing path via the `binaryFile` source (path/size/modificationTime/
  content); matches `file_pattern` (+ `(?P<date>…)`); computes a SHA-256 checksum and
  **skips files already in `{catalog}.silver_control.processed_files`**; parses CSV
  (`file_ingest.parse_csv`) into clean + corrupt rows; a zero-clean-row file is marked
  **CORRUPT** (no write) without breaking other sources. Bronze is written **first**;
  processed-file tracking is advanced only after that write succeeds. Audit columns
  `source_system` / `source_file_name` / `source_file_path` / `file_date` / `run_id` /
  `ingestion_timestamp` / `source_extract_timestamp` / `record_hash` (+ `_corrupt_record`).
  Reuses `sftp_ingest` (checksum/pattern/date/status) + `file_ingest` + `source_config`.
- `pipelines/dag_task.py` dispatches `bronze_sftp` to the real module (removed its
  NOT-IMPLEMENTED branch); `databricks.yml` now passes `--catalog` to the task.
- `seeds/sftp/settlement_2026-06-30.csv` sample drop (4 clean + 1 corrupt row);
  `notebooks/08_validate_sftp_bronze.py` — smoke queries for Bronze rows, the
  processed-files control table, idempotency, and the tracking-after-write invariant.

### Added — Salesforce ingestion (real Spark) — all 6 Bronze sources now execute
- `src/payments_platform/databricks/bronze_salesforce.py` — pulls configured
  Salesforce objects into Bronze Delta via the REST/SOQL API, driven by
  `seeds/salesforce/object_config.json`. Authenticates once per org from the secret
  scope — **configured token flow** (`salesforce-access-token` + instance URL) or
  **OAuth 2.0 username-password flow** (`salesforce-client-id`/`-client-secret`/
  `-username`/`-password`). Incremental via `SystemModstamp`/`LastModifiedDate` pushed
  down into the SOQL `WHERE` (unquoted datetime literal); first run is a full load;
  `queryAll` so soft-deletes come through (`IsDeleted` → `operation_type = DELETE`);
  `nextRecordsUrl` pagination; retry/backoff on 429/5xx. Audit columns
  `source_system` / `source_object` / `run_id` / `batch_id` / `ingestion_timestamp` /
  `source_extract_timestamp` / `record_hash` (+ `_raw_object`, `load_type`,
  `operation_type`). Watermark advanced forward-only **after a successful write** in
  `silver_control.ingestion_watermark`; **per-object try/except** isolation. Reuses
  `source_config` / `salesforce_ingest` constants / `config.secrets` / `rest_ingest`
  retry status / the JDBC watermark helpers.
- `pipelines/dag_task.py` dispatches `bronze_salesforce` to the real module (removed
  its NOT-IMPLEMENTED branch); `databricks.yml` now passes `--catalog` to the task.
- `notebooks/09_validate_salesforce_bronze.py` — smoke queries for the Bronze objects,
  soft-delete capture, and the watermark control table.
- **All six Bronze sources now run for real on Spark**: file (Auto Loader), customer
  CDC → SCD2, JDBC, REST, SFTP, Salesforce — plus governance validation.

### Added — real validation gates (Databricks Spark)
- `src/payments_platform/databricks/gates.py` — `bronze_gate` and `silver_dq_gate`
  run actual checks over the Bronze/Silver + control tables (thresholds reuse the
  tested `orchestration.gates` policies; the Bronze target list + watermark keys come
  from the same source configs the ingestors use).
  - **Bronze**: per-source row counts (required sources > 0), required audit columns
    present, no `CORRUPT` status in `processed_files`, and each watermark not ahead of
    the extracted max.
  - **Silver**: quarantine rate vs threshold, duplicate payment/customer keys, null
    critical columns, and SCD2 validity (one current row per customer,
    `effective_from <= effective_to`, current rows left open, `sequence_number`
    forward-moving, soft-deletes consistent).
- `pipelines/dag_task.py` — the two gate tasks now call the real gates and publish
  the full task-value set (`gate_status` / `gate_passed` / `records_checked` /
  `failed_checks` / `quarantine_rate_pct` / `message`); replaced the static
  `gate_passed=true` / `dq_passed=true` stubs. The condition tasks block downstream on
  `gate_passed`.
- `databricks.yml` — both gate tasks now receive `--catalog`; the Silver condition
  reads `gate_passed` (was `dq_passed`).
- `notebooks/10_validate_gates.py` — smoke validation mirroring every gate check.

### Added — gate results persisted to Delta control tables
- `gates.py` `persist_gate_result` appends each gate outcome to
  `silver_control.pipeline_run_audit` (summary: run_id, task_name, gate_name,
  gate_status, gate_passed, records_checked, failed_checks, quarantine_rate_pct,
  message, check_timestamp), `silver_control.dq_results` (one row per check), and
  `silver_control.table_load_status` (one row per table the gate observed). The gates
  now also return `load_status` per table.
- `pipelines/dag_task.py` — the gate tasks persist the result (best-effort; a failed
  audit write never changes the orchestration outcome) while **keeping the task
  values unchanged** for the condition tasks.
- `notebooks/11_validate_gate_history.py` — latest run status, failed checks,
  quarantine rate, gate history, and table load status.

### Added — pipeline-run audit + monitoring dashboard/alerts
- `gates.py` `record_pipeline_run` / `pipeline_run_status` — `init_run` writes a
  `STARTED` row and `write_status` writes a `SUCCESS`/`PARTIAL` finish row (derived
  from the persisted gate outcomes) to `silver_control.pipeline_run_audit`
  (`gate_name = 'pipeline_run'`), so the audit spans the whole job, not just the gates.
  Wired best-effort in `pipelines/dag_task.py`; `databricks.yml` now passes `--catalog`
  to `write_status`. **Orchestration task values unchanged.**
- `docs/GATE_MONITORING.md` — Databricks SQL dashboard widgets (latest run status,
  bronze/silver gate pass-fail, quarantine-rate trend, failed checks by run, table
  load status by source) and suggested alert SQL (gate failed / quarantine-rate over
  threshold / required source loaded 0 rows), over the `silver_control` audit tables.

### Changed — Databricks-deploy focus; removed local Python test suite
- Removed the pure-Python `tests/` suite and the local in-memory runner pipelines
  (`run_local_slice`, `run_dag_local`, `run_monitored_dag_local`,
  `run_benchmarks_local`, `run_jdbc_ingest_local`). The project is now oriented to
  Databricks deployment; the reusable data logic in `payments_platform.*` is
  imported by the Spark jobs (single source of truth).
- CI reworked to remove `pytest`: `ci.yml` runs dbt parse + generated-SQL drift +
  Terraform validate + bundle validate + credential-free smoke test;
  `deploy.yml`/`terraform.yml` use `validate_deployment.py` + drift checks.
- `pyproject.toml` / `requirements.txt` drop the test tooling (kept `PyYAML` for the
  deploy preflight).

## [1.0.0] — 2026-07-01

First complete release: a governed, multi-source lakehouse on Azure Databricks,
runnable end-to-end locally and deployable to a workspace, proven by **245
pure-Python tests** (no Spark/cloud required).

### Ingestion (Bronze) — 6 active sources, metadata-driven
- ADLS file feed with audit columns + corrupt-record capture.
- Debezium CDC normalization (op/sequence/before-after images).
- **JDBC** (Oracle / SQL Server): full + incremental with watermarks, backfill,
  retries, duplicate-PK handling, For-Each runner over a `source_config` table.
- **REST API**: page + cursor pagination, rate-limit/retry, incremental via
  `updated_since`/cursor, raw-response capture, failed-response handling.
- **SFTP / vendor files**: file-pattern + expected-date validation, checksum
  duplicate detection, corrupt-file handling, raw-to-Bronze copy.
- **Salesforce**: `SystemModstamp`/`LastModifiedDate` incremental, `IsDeleted`
  capture, raw-object capture.
- Secret references resolved by name only (`config/secrets.py`) — never values.

### Silver
- Parse / standardize / type-cast to a conformed schema.
- Data quality with severities (FAIL / QUARANTINE / WARN) + a Failed Record
  Register (full context, status, lineage) — nothing silently dropped.
- Deduplication by business key.
- **CDC apply (SCD2 & SCD1)**: hash-based change detection, soft-delete,
  out-of-order + duplicate handling (provably order-independent).

### Gold (dbt)
- `dim_customer` (+ history), `fact_payments`, `daily_payment_summary`, with tests.

### Governance (Unity Catalog, policy-as-code)
- 5 groups, least-privilege grants, PII tags, column masks, row filters, masked
  views; quarantine raw payload locked to stewards.
- Security validator with negative tests (every guard trips on a broken model).
- Generated UC SQL in `governance/sql/` (drift-checked in CI).

### Orchestration
- Declarative 15-task `investsphere_payments_daily_e2e` DAG: parallel Bronze →
  validation gate → parallel Silver → DQ gate → dbt build/test → governance →
  publish → monitoring write (`ALL_DONE`). Mirrored 1:1 in `databricks.yml`.
- Optional-source failures isolated from the required path at the gate.

### Monitoring, cost & performance
- 9 control/monitoring models, instrumented DAG, 9 alert rules, 6 Databricks SQL
  dashboards.
- Cost estimation by task/source/layer/environment; expensive-task, repeated
  failed-run, and long-running-regression flags.
- Synthetic-data generator + per-stage benchmarks + table-health checks
  (small-file/OPTIMIZE/VACUUM/freshness) + 8 optimization recommendations.

### BI / Power BI
- Analyst-safe serving views (sourced only from marts / masked views / non-PII
  fact) + a semantic model (measures, dimensions, RLS) exported for Power BI.
- Validator guarantees no raw PII is exposed; DirectQuery inherits UC masking + RLS.

### Platform — Terraform + Asset Bundles
- Terraform foundation (resource group, ADLS Gen2, Key Vault, access connector,
  UC catalog/schemas/grants, groups, cluster policy, SQL warehouse, secret scope)
  for dev/test/prod, with grants generated from the governance model.
- Asset Bundle with dev/test/prod targets, the daily DAG + a credential-free
  smoke-test job; test/prod run as the ETL service principal.
- Deployment scripts (`scripts/deploy_bundle.sh`, `scripts/deploy_sql.sh`) and a
  secrets/service-principal preflight.

### CI/CD
- GitHub Actions: pytest, terraform fmt/validate, bundle validate, generated-SQL
  drift checks (governance + monitoring + BI + terraform grants), deploy preflight,
  and the smoke test. Production deploys gated behind a GitHub Environment approval.

### Documentation
- `README.md`, `docs/ARCHITECTURE.md` (Mermaid diagrams), `docs/INTERVIEW_STORY.md`
  (STAR), plus per-area docs: DESIGN, GOVERNANCE, ORCHESTRATION, MONITORING,
  TERRAFORM, DATABRICKS_DEPLOYMENT, PERFORMANCE_COST, POWER_BI, and the four
  per-source ingest guides.

[1.0.0]: https://semver.org/
