# Monitoring & control

The monitoring layer (`src/payments_platform/monitoring/`) is **observability as
policy-as-code**: nine declarative models, a pure-Python recorder that writes them
deterministically, declarative alert rules, and Databricks SQL dashboard queries.
It is **additive** — nothing in `orchestration/` changed; the monitor *wraps* the
existing DAG.

```
monitoring/
  models.py        9 record builders + MODEL_TABLES (model -> schema.table)
  recorder.py      RunMonitor: start/record/finalize API (in-memory ref; Delta in prod)
  alerts.py        9 ALERT_RULES + evaluate_alerts(snapshot)
  dashboards.py    6 Databricks SQL dashboard queries (generate_all -> monitoring/sql/)
  instrument.py    run_monitored_dag: wires the monitor into the orchestration DAG
```

## The nine models → production tables

| Model | UC table | Holds |
|---|---|---|
| `pipeline_run` | `silver_control.pipeline_run` | one row per run: STARTED → SUCCESS/FAILED/PARTIAL |
| `task_run` | `silver_control.task_run` | one row per task **attempt** (retries = attempt 2,3…) |
| `table_load_status` | `silver_control.table_load_status` | row counts + rejected/corrupt per load |
| `dq_results` | `monitoring.dq_results` | DQ rule outcomes (stage = bronze_gate / silver) |
| `table_freshness` | `monitoring.table_freshness` | last-loaded lag vs SLA |
| `quarantine_summary` | `monitoring.quarantine_summary` | quarantine rate + spike flag |
| `dbt_results` | `monitoring.dbt_results` | dbt build/test outcomes |
| `security_events` | `monitoring.security_events` | governance/PII validation results |
| `cost_summary` | `monitoring.cost_summary` | DBUs / spend vs budget |

Run-control models reuse `config.control_tables` (shared with the ingestion path);
the six observability models live in the `monitoring` schema (provisioned by
Terraform, slice 5). All timestamps come from the `RunContext`, so a run records
identically on replay.

## How it wires into the DAG

`run_monitored_dag(handlers, params, ctx, job_name)` wraps each task handler and,
as the existing runner executes the graph, records:

```
init_run              -> pipeline_run STARTED
every task            -> task_run (status, records_written, attempt, error)
bronze_* tasks        -> table_load_status (counts + corrupt/rejected)
bronze_validation_gate-> dq_results (stage=bronze_gate, PASS/FAIL + reasons)
silver_* tasks        -> table_load_status + quarantine_summary
silver_dq_gate        -> dq_results per entity (failed rule + failed count)
dbt_build / dbt_test  -> dbt_results
governance_validation -> security_events (violations -> VIOLATION)
publish / run end     -> pipeline_run finalised SUCCESS / FAILED / PARTIAL
```

**Final status:** `FAILED` if any task hard-failed; `PARTIAL` if a gate blocked
downstream (SKIPPED) with no hard failure; otherwise `SUCCESS`. A normal,
within-threshold quarantine is recorded as a **PASS** dq_results row — only a
gate-breaching entity is FAIL, so healthy runs raise no DQ alert.

The instrumentation lives in `payments_platform.monitoring.instrument.run_monitored_dag`,
exercised credential-free by `pipelines/smoke_test.py`.

## Alert rules (`alerts.py`)

Nine declarative rules, each with a severity, routing channels, and a predicate
over the monitor snapshot. `evaluate_alerts(monitor)` returns the fired alerts.

| Rule | Severity | Fires when | Channels |
|---|---|---|---|
| `pipeline_failure` | CRITICAL | pipeline_run FAILED | email, teams, itsm |
| `bronze_validation_failure` | CRITICAL | bronze_gate dq_results FAIL | email, teams |
| `silver_dq_failure` | HIGH | silver dq_results FAIL | email, teams |
| `dbt_test_failure` | HIGH | dbt test FAILED / tests_failed > 0 | email, teams |
| `quarantine_spike` | HIGH | quarantine rate over spike threshold | email, teams |
| `missing_source_data` | HIGH | load completed with 0 rows written | email, teams |
| `table_freshness_sla` | HIGH | lag > SLA minutes | email, teams |
| `pii_security_violation` | CRITICAL | security_events VIOLATION | email, teams, itsm |
| `cost_threshold_breach` | MEDIUM | spend over budget × threshold | email, teams |

Thresholds (spike %, freshness SLA, cost budget) are evaluated **when the row is
recorded** and stored as a boolean flag, so a number can't be interpreted two ways.

## Dashboards (`dashboards.py` → `monitoring/sql/`)

Six Databricks SQL-ready queries, catalog-parameterised:
`pipeline_health`, `data_quality`, `freshness`, `quarantine`, `security_pii`,
`cost`. Generate per environment:

```bash
python pipelines/generate_monitoring_sql.py investsphere_prod
```

---

## Production mapping: Databricks SQL vs Azure Monitor vs paging

This platform's monitoring has three planes. Put each signal where it belongs.

### What stays in Databricks SQL dashboards (the *analytical* plane)

Trend/history/drill-down that an engineer or steward *looks at*:

- **pipeline_health** — success rate, durations, retry hotspots over 7–30 days.
- **data_quality** — fail rate by rule/entity; which rules quarantine most.
- **freshness / quarantine** — lag and quarantine-rate trends, spike history.
- **security_pii / cost** — violation history; spend vs budget by warehouse.

These read the Delta control/monitoring tables plus the **system tables** in prod:
`system.access.audit` (actual PII access on `silver_cdc` / `gold.dim_customer`),
`system.billing.usage` (DBUs by job), `system.lakeflow.job_run_timeline` (run
durations) — the dashboard SQL includes commented joins showing how. Dashboards
are not a pager; nobody watches them at 3am.

### What is sent to Azure Monitor / Log Analytics (the *operational* plane)

Discrete events that must **trigger** something. The Lakeflow job's final task
emits the monitor's records + `evaluate_alerts` output to Log Analytics via the
Data Collector API / a diagnostic setting, landing in custom tables:

- `pipeline_run` final status, `task_run` failures/retries → `InvestSpherePipeline_CL`
- fired alerts (id, severity, channels, offenders) → `InvestSphereAlerts_CL`
- Databricks workspace **diagnostic logs** (clusters, jobs, UC) → native tables

Azure Monitor owns **alert rules, action groups, and escalation** — the same
conditions `alerts.py` encodes, expressed as KQL scheduled-query alerts so they
fire even if a run never completes (e.g. a missing-run / heartbeat alert that
Databricks-internal logic can't raise about itself).

### What pages whom (the *response* plane)

| Severity | Examples | Route |
|---|---|---|
| CRITICAL | pipeline_failure, bronze_validation_failure, **pii_security_violation** | Action group → **email + Teams + ITSM (ServiceNow) incident**; PagerDuty on-call out of hours |
| HIGH | silver_dq_failure, dbt_test_failure, quarantine_spike, freshness SLA, missing source | **Email + Teams** to the data-platform channel; ITSM ticket if unacked in N min |
| MEDIUM | cost_threshold_breach | **Email + Teams** to FinOps/owner; no page |

PII/security violations are wired to ITSM **and** Teams because they are
compliance events, not just ops noise — they need an auditable incident record.

### Why three planes, not one

Dashboards answer *"is the platform healthy and trending well?"* (pull, history).
Azure Monitor answers *"did a specific bad thing just happen, and did we tell
someone?"* (push, real-time, survives a dead run). Keeping alert **definitions**
in `alerts.py` (tested here) and alert **delivery** in Azure Monitor action groups
means the same policy is provable in CI and enforced in prod without duplication.

## How this maps to real Databricks primitives

| Reference (here) | Production |
|---|---|
| `RunMonitor` in-memory tables | MERGE into Delta `silver_control.*` / `monitoring.*` |
| `record_task` attempts | Lakeflow task **retry policy** + `system.lakeflow.job_run_timeline` |
| `table_freshness` builder | Lakehouse Monitoring / a freshness job vs `_loaded_at` |
| `quarantine_summary` | aggregate over `silver_quarantine.failed_records` |
| `security_events` | `governance.validate_all()` in the job + `system.access.audit` |
| `cost_summary` | `system.billing.usage` joined to job/warehouse tags |
| `evaluate_alerts` | Databricks SQL Alerts **and** Azure Monitor KQL alerts |

## Guaranteed behavior

The monitoring model enforces: a successful run writes correct statuses + no alerts; a failed Bronze gate
blocks downstream and records a bronze_gate FAIL (run = PARTIAL); a Silver DQ
failure records the failed rule + failed count; a dbt test failure marks the run
FAILED; a governance failure records a VIOLATION security event; freshness breach
and quarantine spike fire their alerts; task retries are recorded as separate
attempts; final status (SUCCESS/PARTIAL/FAILED) is derived correctly; a healthy
quarantine does **not** raise a DQ alert; and all 6 dashboards substitute the
catalog.
