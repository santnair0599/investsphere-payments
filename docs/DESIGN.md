# InvestSphere — full platform design

> This project evolved from a payments-practice foundation into an enterprise
> business AI decision platform. The original ingestion and lakehouse patterns were
> preserved and generalized across enterprise domains.

Target design for the complete platform (bundle/package name `investsphere_payments`,
an internal identifier retained from the payments-practice foundation). The repo
grew from a **thin vertical slice** (the retained payments-practice path: file feed +
customer/guest CDC → Bronze → Silver → dbt Gold) into the enterprise design this
document describes; see [`ENTERPRISE_PIVOT.md`](ENTERPRISE_PIVOT.md) for the canonical
six-source → domain mapping.

**Domain:** InvestSphere is a **diversified investment holding company** spanning
**real estate, hospitality, entertainment, and investment** assets. The Lakehouse
consolidates operational and financial data across these domains on Azure Databricks
(Unity Catalog, Lakeflow, dbt, Terraform, Asset Bundles), and an Azure AI Foundry /
LangGraph agent serves grounded, trust-gated business recommendations on top of the
governed Gold marts.

## Sources & ingestion matrix

| Source | Connect | Load | CDC/delete | Notes |
|---|---|---|---|---|
| Oracle (real-estate PMS) | JDBC | incremental (watermark) / CDC | CDC seq | properties/leases/occupancy/maintenance; partitioned reads |
| SQL Server (investment/treasury) | JDBC | incremental / SQL Server CDC | op codes | assets/performance/risk_exposure/cashflow; self-hosted gateway |
| ADLS files (CSV/JSON/PDF) | Auto Loader | incremental files | n/a | marketing/campaign exports; `_rescued_data`, checkpoints |
| CDC feeds (Debezium/DB CDC/Delta CDF) | Kafka / CDF | streaming | op=c/u/d, tombstone | customer/guest master; Bronze stores events only |
| Kafka | Structured Streaming | streaming | event keys | offset checkpointing |
| REST API (hospitality booking + FX) | requests | incremental (page token) | status flags | hotels/bookings/revenue/fx_rates; pagination, rate limits, retries |
| Salesforce (enterprise CRM) | managed connector | incremental (SystemModstamp) | IsDeleted | Account=segment / Contact=guest / Opportunity=deal / Case=guest review |
| SFTP / vendor files (entertainment ticketing) | SFTP + Auto Loader | per-file | delete flag/status | venues/ticket_sales/footfall; filename pattern + dup-file check |

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
**business marts** across six Gold schemas — `gold_realestate.mart_property_underperformance`,
`gold_hospitality.mart_hotel_revenue_risk`, `gold_entertainment.mart_venue_conversion_risk`,
`gold_investment.mart_investment_risk`, `gold_customer.mart_declining_customer_segments`,
and `gold_ops_trust` (pipeline/DQ trust from `silver_control.*`) — → dbt tests + docs.
Each `mart_*` is the business-question answer surface the AI agent's SQL tools hit.

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
