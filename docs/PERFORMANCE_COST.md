# Performance, cost & scalability

The `perf/` package validates the platform's performance, cost, and scalability
*behaviour* with a deterministic, pure-Python harness — then maps each local
signal to its Databricks production equivalent. It is additive and reference-only:
the numbers are illustrative, the **decisions and attribution model** are the point.

```
src/payments_platform/perf/
  synth.py            deterministic synthetic data (dup/corrupt/invalid/late) sized small/medium/large
  benchmark.py        times each medallion stage + writes monitoring rows
  cost.py             estimate spend by task/source/layer/env; flag expensive/failed/regressed
  table_health.py     file-count simulation; small-file/OPTIMIZE/VACUUM/freshness flags
  recommendations.py  the 8 optimization recommendations + a workload-profile matcher
```

These `payments_platform.perf.*` modules are the cost/health reference models; on
Databricks the signals come from `system.billing.usage` + table stats (see below).

## What is benchmarked locally

| Stage (`benchmark.py`) | What it times | Reference module exercised |
|---|---|---|
| `bronze_file_ingest` | parse CSV + capture corrupt + add audit cols | `bronze/file_ingest` |
| `jdbc_incremental` | metadata-driven For-Each over N source tables | `bronze/jdbc_ingest` |
| `cdc_scd2_apply` | SCD2 apply over an out-of-order, duplicated CDC stream | `silver/cdc_apply` |
| `silver_dq_quarantine` | dedup + DQ rules + quarantine split | `silver/dedup,dq,quarantine` |
| `dbt_gold_build_sim` | a Gold aggregation (revenue per customer/currency) | (simulation) |
| `end_to_end_dag` | the full 15-task monitored DAG over in-memory sources | `orchestration` + `monitoring` |

Each run writes real monitoring rows (`table_load_status` per table, one
`quarantine_summary`, `dbt_results`, and `cost_summary` per timed task), so a
benchmark is observable exactly like a production run. **Timings are
informational** — tests assert that rows are written and that scalability is
*correct* (e.g. CDC converges regardless of order), never wall-clock values.

### Scalability properties proven

- **Metadata-driven ingestion scales** — `make_jdbc_configs(N)` + the For-Each
  runner ingest N configured tables and write one control row per table.
- **CDC is order-independent at volume** — `apply_scd2` over a shuffled, duplicated
  stream yields the **same current state** as the ordered stream (hash + sequence
  guards), proven at `medium` size.
- **Optional source failures don't block required processing** — a failing REST
  source leaves the Bronze gate (which depends only on required sources) passing
  and Silver/Gold proceeding; it only blocks if added to the gate's
  `required_sources`.

## Cost observability

`cost.py` estimates `(dbus, cost_usd)` from `duration_ms × DBU/hour × $/DBU` per
compute type, attributes it by **task / source / layer / environment**, and flags:

- **expensive tasks** (`flag_expensive_tasks`, threshold on `cost_usd`),
- **repeated failed-run cost** (`flag_repeated_failed_run_cost` — wasted spend on
  FAILED runs),
- **long-running regressions** (`flag_long_running_regression` vs a baseline).

Estimates are written to `monitoring.cost_summary` via `RunMonitor.record_cost`,
so the existing `cost_threshold_breach` alert fires on a budget breach. All-purpose
compute is modelled as the most expensive per DBU — the model nudges toward
job/serverless.

## Table health

`table_health.py` simulates Delta file layout from row counts and flags:
small files (→ **OPTIMIZE**), accumulated tombstones with no recent vacuum (→
**VACUUM**), and stale freshness (→ SLA breach, reusing `monitoring.table_freshness`).

## Mapping to Databricks production signals

| Local (reference) | Databricks production |
|---|---|
| `benchmark.timed(...)` per-stage ms | **`system.lakeflow.job_run_timeline`** + **Query History** (per-task / per-query latency) |
| `cost.estimate_cost` (DBU × rate) | **`system.billing.usage`** joined to job/warehouse tags (`custom_tags.project/environment/cost_center`) |
| `cost_summary` rows + budget breach | a **Databricks SQL Alert** / Azure Monitor KQL alert on the billing rollup |
| `table_health.file_count_simulation` | **`DESCRIBE DETAIL`** (`numFiles`, `sizeInBytes`) / file listings |
| small-file → OPTIMIZE | **`OPTIMIZE`** (bin-packing) + **Predictive Optimization** auto-compaction |
| tombstones → VACUUM | **`VACUUM`** (retention) — automated by Predictive Optimization on managed tables |
| stats staleness | **`ANALYZE TABLE … COMPUTE STATISTICS`** (or auto-stats) feeding the CBO |
| `dbt_gold_build_sim` aggregation | a **Photon**-accelerated SQL warehouse build; Photon vectorizes scans/aggregations |
| `make_jdbc_configs(N)` For-Each | a Lakeflow **For-Each task** over the `source_config` control table |
| large Gold table | **Liquid Clustering** (`CLUSTER BY [AUTO]`) instead of static partitioning |
| serverless vs all-purpose cost | **serverless jobs / job clusters** (cheaper, ephemeral) over all-purpose |

## Optimization recommendations (`recommendations.py`)

Eight recommendations, each with a predicate over a workload profile:
Auto Loader for files · incremental/CDC over full reload · dedup before MERGE ·
hash-based change detection · Liquid Clustering / `CLUSTER BY AUTO` for large Gold ·
Predictive Optimization for managed tables · job/serverless over all-purpose ·
auto-termination + cost tags.

## Interview-ready summary

**Performance.** Ingestion is incremental by default — watermark-driven JDBC and
`updated_since`/cursor REST read only changed rows; CDC uses **sequence ordering +
SHA-256 change detection** so unchanged updates never rewrite history. Silver
**dedups before MERGE** to keep merges deterministic and cut write amplification.
In production the heavy lifting runs on a **Photon** SQL warehouse (vectorized
scans/aggregations) and **Auto Loader** for incremental file discovery with schema
evolution.

**Cost.** Spend is attributed by task → source → layer → environment from
**`system.billing.usage`** joined to mandatory `project/environment/cost_center`
tags (set by the Terraform cluster policy). The biggest levers: **serverless/job
compute** instead of always-on all-purpose clusters, **auto-termination** to kill
idle compute, **incremental over full reload** to shrink scanned bytes, and
**Predictive Optimization** so OPTIMIZE/VACUUM/ANALYZE run only when they pay off.
We alert on budget breaches and on **wasted spend from repeated failed runs**.

**Scalability.** The pipeline is **metadata-driven** — adding a source table is a
config row, fanned out by a Lakeflow **For-Each**, so it scales horizontally
without new code. Bronze sources run in parallel and optional-source failures are
isolated from required ones at the gate. For large Gold tables, **Liquid
Clustering (`CLUSTER BY AUTO`)** replaces static partitioning to avoid small-file
skew on high-cardinality keys, and the CDC engine is provably **order-independent**,
so late/duplicate events at volume still converge to the correct state.
