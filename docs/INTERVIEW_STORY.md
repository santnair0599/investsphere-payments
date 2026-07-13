# Interview story — InvestSphere

> This project evolved from a payments-practice foundation into an enterprise
> business AI decision platform. The original ingestion and lakehouse patterns were
> preserved and generalized across enterprise domains.

Interview-ready talking points for this project (bundle/package name
`investsphere_payments`, an internal identifier retained from the payments-practice
foundation), in **STAR** format (Situation,
Task, Action, Result). Stories are sequenced in **build order** — ingestion →
transformations → quality → governance → orchestration → observability →
platform → serving — so you can walk an interviewer through the platform the way
it was built. Pair with the diagrams in [ARCHITECTURE.md](ARCHITECTURE.md).

## Elevator pitch

**30 seconds.** "InvestSphere is a governed, multi-source lakehouse on Azure
Databricks for a **diversified investment holding company** — real estate,
hospitality, entertainment, and investment. Six sources land in Bronze, get cleaned
and conformed in Silver — including SCD2 CDC with out-of-order handling — then dbt
builds the Gold business marts, which Power BI consumes through masked, PII-safe
serving views. On top, an **Azure AI Foundry / LangGraph agent** answers leadership
questions grounded in those marts, trust-gated by pipeline and data-quality signals.
Unity Catalog governance, monitoring/cost, and the whole Terraform + Asset Bundle
deployment are all policy-as-code, and the data logic lives in one reusable Python
library that the Spark jobs import — a single source of truth from design to the
Databricks runtime."

**2 minutes.** Add the *why*: I wanted to prove every hard mechanic of a
production lakehouse — metadata-driven ingestion, correct CDC, data-quality gating,
PII governance, observability, IaC, and a BI contract — without paying for a
cluster on every commit. So the data logic (CDC ordering, SCD2,
DQ, masking, cost) is written once as a **clean, reusable Python library**, and the
same functions are imported by the Spark jobs that run on Databricks — Auto Loader
Bronze, Delta MERGE Silver, and the governance gate — so the runtime can't drift
from the design. The result is a platform that's both runnable end-to-end
locally and deployable to a real workspace via Terraform → generated SQL → Asset
Bundle → smoke test.

---

## STAR stories

### 1. Metadata-driven multi-source ingestion

- **S** — Enterprise data arrives from six very different sources: an ADLS campaign
  file feed (Autoloader), the real-estate PMS and investment/treasury systems via
  JDBC (Oracle/SQL Server), Debezium CDC on the customer/guest master, a hospitality
  booking REST API, SFTP entertainment-ticketing vendor files, and a Salesforce CRM.
  Hand-writing a bespoke ingestor per source doesn't scale and rots.
- **T** — Make adding or changing a source a *configuration* change, not a code
  change, while still handling each source's quirks (pagination, watermarks, file
  patterns, soft-deletes).
- **A** — Built a **metadata-driven** ingestion layer: a `source_config` table
  drives a **For-Each** runner; each source type abstracts its transport behind a
  small client interface so it runs against an in-memory fake or the real service. JDBC does
  full/incremental with **watermarks + backfill + retries + dup-PK** handling; REST
  does page/cursor pagination with rate-limit retry and raw-response capture; SFTP
  does file-pattern/expected-date validation, checksum dedup, and corrupt-file
  handling; Salesforce does `SystemModstamp` incremental with `IsDeleted` capture.
- **R** — Adding a source is a config row fanned out by the For-Each task; the
  previously-disabled REST/SFTP/Salesforce sources were lit up additively with zero
  changes to the core. **48 ingestion tests**; optional-source failures are
  isolated from the required ones at the gate.

> *Talking point:* "The spine is source-agnostic. In production the For-Each maps to
> a Lakeflow For-Each task over the `silver_control.source_config` Delta table."

### 2. Correct CDC — out-of-order, duplicates, and no spurious history

- **S** — Debezium delivers change events that can arrive **out of order**,
  **duplicated**, or replayed, and a naive SCD2 implementation either corrupts
  history or rewrites it on every no-op update.
- **T** — Maintain correct SCD2 history and SCD1 current-state regardless of arrival
  order, without rewriting history when nothing actually changed.
- **A** — The CDC engine orders by a **sequence number**, uses a **SHA-256 hash of
  the tracked columns** to detect *real* change (expire-and-insert only on a hash
  difference), guards against stale UPDATE/DELETE events older than the key's
  creation, ignores events not newer than the current version, and models deletes
  as **soft-delete / SCD2-expire**.
- **R** — Provably **order-independent**: a test feeds a shuffled, duplicated stream
  at scale and asserts the same current state as the ordered stream. This is the
  pure-Python equivalent of Databricks **AUTO CDC** (`APPLY CHANGES ... SEQUENCE BY
  ... STORED AS SCD TYPE 2`).

> *Likely question:* "How do you handle late-arriving CDC events?" → sequence
> ordering + create-sequence stale guard + hash change-detection; deterministic and
> idempotent on replay.

### 3. Data quality you can't silently lose

- **S** — Bad records (negative revenue, invalid occupancy rates, disallowed
  currencies, missing business keys) must never reach the Gold marts — but they also
  must never be silently dropped in a governed enterprise domain.
- **T** — Separate good from bad, keep full context on the bad, and stop unauditable
  data hard.
- **A** — DQ rules carry **severities** (FAIL / QUARANTINE / WARN). A FAIL breach
  (e.g. a missing entity/business key) raises and stops the job; QUARANTINE rows are routed
  to a **Failed Record Register** with the failing rule, reason, raw payload,
  lineage, and an `OPEN` status; WARN rows pass but are counted. A **Silver DQ gate**
  then blocks promotion to Gold on quarantine-rate or FAIL-severity thresholds.
- **R** — Nothing is lost; bad records are queryable and replayable; the gate
  prevents a bad batch from polluting Gold. Maps to Lakeflow expectations
  (`expect_or_drop` / `expect_or_fail`) + a quarantine table.

### 4. PII governance as policy-as-code

- **S** — Customer data carries PII (name, email, phone, national/passport/Emirates
  ID). Engineers and analysts must do their jobs **without ever seeing raw PII**, and
  I have to *prove* that, not assert it.
- **T** — Enforce least-privilege + masking + row-level region filtering in Unity
  Catalog, generated from one declarative model, and test that the guarantees hold.
- **A** — A single **declarative governance model** (groups, PII classification,
  masks, row filters, masked views, grants) generates the Databricks UC SQL. A
  **security validator** checks the invariants — *no engineer/analyst SELECT on a PII
  base table*, *every PII column has a mask*, *quarantine raw payload locked to
  stewards*, *masked views expose no raw PII*. Each check has a **negative test** that
  proves it trips on a deliberately broken model.
- **R** — Engineers get hashed join keys, analysts get display-masked + region-filtered
  views, only the ETL service principal writes base tables. **21 governance tests**;
  the same enforcement is inherited by SQL, notebooks, and Power BI.

> *Talking point:* "The grants Terraform applies are *generated from the same
> governance model*, so infra can't drift from the security policy — CI fails on
> drift."

### 5. Orchestration with gates and failure isolation

- **S** — A daily pipeline with parallel sources and quality gates needs to fan out,
  gate between layers, and survive partial failures.
- **T** — Model the workflow declaratively, run it both locally and as a Lakeflow
  Job, and make sure an optional source failing doesn't take down the run.
- **A** — A **declarative DAG** (22 tasks) mirrored 1:1 in `databricks.yml`: parallel
  Bronze → **validation gate** → parallel Silver → **DQ gate** → dbt build/test →
  governance validation → publish → monitoring write (`ALL_DONE`). Gates publish a
  task value a condition task branches on. The Bronze gate depends only on the
  **required** sources, so a failed REST/SFTP/Salesforce load is recorded but doesn't
  block downstream — unless added to `required_sources`.
- **R** — Graph semantics (ordering, parallelism, gate-blocks-downstream,
  failure-isolation) are declared in one place (`orchestration/dag.py`); the Asset
  Bundle deploys that exact graph as the Lakeflow Job.

### 6. Observability and cost as code

- **S** — Operators need to know *is it healthy?* and *what does it cost?*, and
  finance needs spend attributed.
- **T** — Capture run health, data quality, freshness, and cost, alert on problems,
  and feed dashboards.
- **A** — Nine control/monitoring models (pipeline/task/load-status, DQ, freshness,
  quarantine, dbt, security, cost); the DAG is **instrumented** to write them; **9
  declarative alert rules** route by severity to email/Teams/ITSM; **6 Databricks SQL
  dashboards**. A cost module estimates spend from duration × compute type, attributes
  it by **task/source/layer/environment**, and flags expensive tasks, repeated
  failed-run cost, and long-running regressions.
- **R** — A benchmark run is observable exactly like a real run; in production these
  map to `system.lakeflow.job_run_timeline`, `system.billing.usage`, and Query
  History. **22 monitoring + 25 performance/cost tests**.

> *Likely question:* "How do you control Databricks cost?" → serverless/job over
> all-purpose, auto-termination + cost tags via cluster policy, incremental over full
> reload, Predictive Optimization, and budget/regression alerts off the billing
> system tables.

### 7. Reproducible deployment — Terraform + Asset Bundles

- **S** — The platform has to deploy to dev/test/prod reproducibly, with the right
  identity and no secrets in git.
- **T** — Separate long-lived infrastructure from application jobs, and make prod safe.
- **A** — **Terraform owns** the resource group, ADLS Gen2, Key Vault, UC
  catalog/schemas/grants, groups, cluster policy, SQL warehouse, and the KV-backed
  secret scope; the **Asset Bundle owns** the jobs. Terraform **outputs** feed bundle
  variables (catalog, warehouse, secret scope, service principal). dev runs as the
  deployer; **test/prod run as the ETL service principal**. CI runs dbt parse, terraform
  validate, bundle validate, **generated-SQL drift checks**, and a smoke test; prod
  deploys are gated behind a **GitHub Environment approval**.
- **R** — "Terraform builds the house, the bundle moves in." One-command deploy per
  env; a credential-free **smoke-test job** verifies wiring end-to-end. **39 terraform
  + 17 bundle/deploy tests**.

### 8. A governed BI serving contract

- **S** — Power BI must serve enterprise business analytics **without ever exposing
  PII**, and metric definitions shouldn't live only inside a `.pbix`.
- **T** — Give BI a thin, safe, governed serving layer and a versioned semantic model.
- **A** — Serving views in the analyst-granted schema source **only** marts, masked
  views, or the non-PII fact — never a PII base table — and a validator fails the build
  otherwise. A **semantic model as code** (measures, dimensions, RLS) is exported for
  Power BI. With **DirectQuery**, Unity Catalog enforces masking + the region row
  filter at query time, so the BI tool physically can't see unmasked PII.
- **R** — Safe-by-construction BI; the same governance protects SQL, notebooks, and
  Power BI identically. **16 BI tests**.

---

## Architecture talking points (60-second walk)

"Six sources → **Bronze** (audit columns, corrupt capture) → **Bronze validation
gate** → **Silver** (parse, dedup, DQ/quarantine, SCD2 CDC apply) → **Silver DQ
gate** → **Gold** (dbt dim/fact/marts + tests) → **BI** masked serving views.
Cross-cutting: **Unity Catalog governance** (masks, row filters, least-privilege
grants), **monitoring + cost** (control tables, alerts, dashboards). Underneath:
**Terraform** for infra and **Asset Bundles** for jobs, dev/test/prod, CI/CD. The
data logic is a reusable Python library imported by the Spark jobs; each module
documents its Databricks/Spark counterpart."

## Q&A cheat-sheet

| Question | One-line answer |
|---|---|
| Why medallion? | Isolate raw landing (replayable) from cleaning from business marts; each layer has one job and its own quality bar. |
| Late / out-of-order CDC? | Sequence ordering + create-seq stale guard + hash change-detect; provably order-independent, idempotent on replay. |
| Stop analysts seeing PII? | Policy-as-code UC masks + row filters + masked views; no engineer/analyst SELECT on PII base; the `governance_validation` gate fails the run on any violation. |
| Iterate without a cluster? | The data logic is a standalone Python library; the credential-free `smoke_test.py` exercises the whole DAG with in-memory sources, and CI validates the bundle, Terraform, dbt parse, and generated SQL — none needs Spark. |
| Control cost? | Serverless/job compute, auto-termination + cost tags, incremental over full reload, Predictive Optimization, budget/regression alerts off billing system tables. |
| Scale to many sources/volumes? | Metadata-driven config + For-Each (horizontal), parallel Bronze, Liquid Clustering on large Gold, order-independent CDC. |
| Terraform vs Asset Bundle? | Terraform owns infra (catalog/schemas/grants/warehouse/secret scope); bundle owns jobs; outputs feed bundle vars. |
| Bad records? | Severities (FAIL/QUARANTINE/WARN); FAIL stops the job, QUARANTINE to a Failed Record Register with full context; gate blocks promotion. |
| How is governance not bypassable by BI? | DirectQuery runs in UC as the analyst, so masks + row filters apply at query time; serving views only ever expose marts/masked views. |
| Schema drift / data evolution? | Auto Loader `_rescued_data` / `_corrupt_record` capture in Bronze; conform in Silver; nothing dropped silently. |

## Decisions & trade-offs (be ready to defend)

- **Pure-Python reference + documented Spark counterpart** vs running real Spark in
  CI — chose fast, deterministic, cluster-free tests; trade-off is the reference isn't
  the literal prod engine, mitigated by documenting the exact Spark mapping per module.
- **Gate depends only on required sources** — optional sources can fail without
  blocking the business-critical path; trade-off is you must consciously promote a
  source to "required" when it becomes critical (a one-line policy change).
- **Policy-as-code with generated SQL + drift checks** — single source of truth for
  governance/grants/monitoring/BI; trade-off is a generation/CI step, bought back by
  never having infra drift from the security model.
- **Managed tables + Predictive Optimization** over hand-tuned maintenance — less
  operational toil; trade-off is less manual control, acceptable for this workload.
- **DirectQuery for governed BI** over Import — governance enforced at query time;
  trade-off is query latency, mitigated by serving pre-aggregated marts on Photon.

## Numbers to cite

6 active Bronze sources · 22-task orchestration DAG · 5 Unity Catalog groups ·
9 monitoring models · 9 alert rules · 6 SQL dashboards · 8 optimization
recommendations · 3 BI serving datasets · dev/test/prod · **credential-free smoke test + generated-SQL drift in CI.**
