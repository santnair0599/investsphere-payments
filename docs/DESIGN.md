# InvestSphere Payments — full platform design

Target design for the complete platform. The repo currently implements the
**thin vertical slice** (payments file + customer CDC → Bronze → Silver → dbt
Gold); this document is the blueprint the slice grows into.

**Domain:** Financial Services / Investment & Payments data platform on Azure
Databricks (Unity Catalog, Lakeflow, dbt, Terraform, Asset Bundles).

## Sources & ingestion matrix

| Source | Connect | Load | CDC/delete | Notes |
|---|---|---|---|---|
| Oracle (Azure) | JDBC | incremental (watermark) / CDC | CDC seq | partitioned reads |
| SQL Server (on-prem) | JDBC | incremental / SQL Server CDC | op codes | self-hosted gateway |
| ADLS files (CSV/JSON/PDF) | Auto Loader | incremental files | n/a | `_rescued_data`, checkpoints |
| CDC feeds (Debezium/DB CDC/Delta CDF) | Kafka / CDF | streaming | op=c/u/d, tombstone | Bronze stores events only |
| Kafka | Structured Streaming | streaming | event keys | offset checkpointing |
| REST API | requests | incremental (page token) | status flags | pagination, rate limits, retries |
| Salesforce | managed connector | incremental (SystemModstamp) | IsDeleted | Account/Contact/Opportunity |
| SFTP / vendor files | SFTP + Auto Loader | per-file | delete flag/status | filename pattern + dup-file check |

Cross-cutting per source: connection method · auth/secrets (Key Vault scope) ·
full vs incremental · CDC/delete handling · schema changes · duplicates ·
late/out-of-order · bad-record quarantine · checkpointing · retries · backfill ·
audit columns · DQ · cost/perf.

## Bronze — land raw, full traceability, no loss

Audit columns: `source_system, source_file_name, source_file_path,
ingestion_timestamp, batch_id, run_id, record_hash, operation_type,
source_extract_timestamp, _corrupt_record`. No heavy transforms, no SCD.

## Silver — clean, validated, deduplicated, conformed

Parse/flatten/cast → standardize names/types → **DQ checks** (FAIL/QUARANTINE/
WARN) → split valid/quarantine → dedup → **CDC apply (SCD1/SCD2)** → delete
handling (soft / SCD2-expire) → late/out-of-order via sequence + watermark →
conform across sources. Schemas: `silver_clean`, `silver_cdc`, `silver_quarantine`,
`silver_control`.

## Gold — dbt on trusted Silver

`sources.yml` → staging (views, DQ-passed) → intermediate → **dimensions**
(current from SCD1/SCD2-current; history from SCD2) + **facts** (incremental) →
**marts** (customer_360, daily_revenue, risk_exposure, dq_scorecard) → dbt tests
+ docs.

## Governance (Unity Catalog)

Classify PII → Entra ID groups → controlled ownership (not data engineers) →
least-privilege grants → **column masks** (email/phone/Emirates ID/passport) →
**row filters** (country/dept) → **masked views** for engineers/analysts →
hash/tokenize for joins → lock down Bronze PII + quarantine raw payload → audit
logs + lineage → Key Vault secrets + storage credentials/external locations.
Catalogs: `*_raw_restricted`, `*_silver`, `*_gold` (secure/masked/public).

## Orchestration (Lakeflow Jobs)

Parallel Bronze (all sources) → **Bronze validation gate** → Silver (parse/DQ/
quarantine/dedup/CDC-SCD) → **Silver DQ gate** → dbt build → dbt test →
governance check → publish/notify. Triggered for batch, continuous for streaming.
For-Each over a `source_config` table for many tables. Separate workflows:
`batch_daily_e2e`, `kafka_streaming`, `maintenance` (OPTIMIZE/VACUUM), `backfill`.
Service-principal run-as; retries/timeouts/alerts; control tables for restart/RCA.

## Monitoring

Five levels: job/task health · DQ + quarantine · freshness/volume · perf/cost ·
security/PII access. Sources: Workflows UI, UC **system tables** (`system.billing.usage`,
`system.lakeflow.*`, `system.query.history`, `system.access.audit`), Lakeflow event
logs, Databricks SQL dashboards, Azure Monitor/Log Analytics alerting.

## CI/CD

- **Terraform** = platform/infra/governance (workspace, UC catalogs/schemas/
  external locations, storage credentials, groups/grants, SQL warehouses, cluster
  policies, secret scopes).
- **Asset Bundles** = Databricks code: jobs, Lakeflow pipelines, notebooks/wheels,
  dbt task.
- **dbt** = Gold SQL build/test/docs. **GitHub Actions** = runner.
- PR: `terraform validate` + `bundle validate` + `dbt parse` + generated-SQL drift
  + secret scan + smoke test. Promote dev → test → prod (manual gate).

## Performance & cost (priority order)

1. Auto Loader (checkpoints/schemaLocation) for files.
2. Incremental/CDC instead of full loads for DBs.
3. Delta maintenance: predictive optimization, OPTIMIZE/VACUUM/ANALYZE.
4. Liquid Clustering / `CLUSTER BY AUTO` on large tables (not high-cardinality
   partitioning).
5. Dedup before MERGE + `record_hash` change detection (skip no-op updates).
6. dbt incremental models for large facts.
7. Jobs/serverless compute + Photon + auto-termination.
8. Cluster policies + cost-attribution tags.
9. Monitoring via system tables.
10. Metadata-driven ingestion/DQ for scale (config tables, For-Each, reusable
    frameworks).
