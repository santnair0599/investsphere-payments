# Orchestration — `investsphere_payments_daily_e2e`

> This project evolved from a payments-practice foundation into an enterprise
> business AI decision platform. The original ingestion and lakehouse patterns were
> preserved and generalized across enterprise domains (real estate, hospitality,
> entertainment, investment, customer/CRM, ops-trust).

The full daily workflow, deployed as a **Databricks Asset Bundle** job
(`databricks.yml`, bundle name `investsphere_payments` — internal name, retained from
the payments-practice origin) and modelled as testable policy-as-code
(`src/payments_platform/orchestration/`). The six Bronze sources fan out across the
enterprise domains; the medallion, gates, SCD2, and dbt Gold structure is unchanged.

```
init_run
   │
   ├────────────┬───────────┬──────────┬────────┬──────────────┐   (Bronze — parallel)
bronze_payments bronze_jdbc bronze_     bronze_  bronze_  bronze_customer_cdc
   file         (For-Each)  rest_api    sftp     salesforce  (Debezium)
   └────────────┴───────────┴──────────┴────────┴──────────────┘
                   │   (gate depends only on the REQUIRED sources, not all six)
        bronze_validation_gate ── bronze_gate_check (condition)
                   │ true
        ┌──────────┴───────────┐               (Silver — parallel)
   silver_payments        silver_customer_scd2
        └──────────┬───────────┘
                   │
        silver_dq_gate ── silver_dq_check (condition)
                   │ true
              dbt_build ──► dbt_test           (Gold; test failure fails job)
                   │
        governance_validation                  (fails job on PII/access violation)
                   │
             publish_notify
                   │
              write_status                      (run_if ALL_DONE — monitoring)
```

## Orchestration on Databricks

The DAG is declared in `payments_platform.orchestration.dag` and deployed as a
Lakeflow Job by the Asset Bundle (`databricks.yml`), where each task runs
`pipelines/dag_task.py --task <name>`. Gates publish a task VALUE that a
condition task branches on. Validate the wiring with `databricks bundle validate -t dev`.

## Mapping to Databricks Workflows / Lakeflow Jobs

| Model concept | Databricks |
|---|---|
| `dag.TASKS` | the job's `tasks:` with `depends_on` |
| `parallel_groups()` (level 1) | bronze tasks sharing `depends_on: init_run` → run concurrently |
| condition gate (`kind: condition`) | a `condition_task` (`EQUAL_TO`) branching on a task **value** |
| gate `outcome=True` dependency | `depends_on: [{task_key: …_check, outcome: "true"}]` |
| `runner` skipping downstream | Databricks skips tasks whose condition/parent isn't satisfied |
| `_read_with_retry` / task retries | task `max_retries` (the `*retry` anchor) |
| `write_status` (ALL_DONE) | `run_if: ALL_DONE` so control rows are written even on failure |
| dbt build/test | `dbt_task` against a SQL warehouse (`warehouse_id`) |

### Gates as task values

A gate is a Python task that computes pass/fail and publishes a **task value**:

```python
dbutils.jobs.taskValues.set(key="gate_passed", value="true")
```

then a **condition task** branches on it:

```yaml
- task_key: bronze_gate_check
  condition_task:
    op: EQUAL_TO
    left: "{{tasks.bronze_validation_gate.values.gate_passed}}"
    right: "true"
- task_key: silver_payments
  depends_on: [{ task_key: bronze_gate_check, outcome: "true" }]
```

If the gate fails, `bronze_gate_check` is false → every Silver/Gold/governance
task downstream is **skipped** (enforced by the condition task + `run_if` in the job).

## Job parameters

`env`, `catalog`, `run_date`, `run_id`, `load_mode` (`incremental` | `full` |
`backfill`), `backfill_start_date`, `backfill_end_date`, `secret_scope`.

`load_mode=backfill` + the two dates flow to `bronze_jdbc`, which builds the
backfill window for the metadata-driven JDBC ingestor (`test_backfill_params_
passed_to_jdbc_ingestor`). Backfill reads the historical window **without moving
the forward watermark**, so daily runs are unaffected.

## Gates (policy)

- **Bronze validation** (`gates.bronze_validation_gate`): required sources loaded
  SUCCESS, no FAILED ingestion, total corrupt ≤ threshold, row count ≥ minimum.
- **Silver DQ** (`gates.silver_dq_gate`): quarantine rate ≤ ceiling, no
  FAIL-severity breach.

### Active Bronze sources

All six Bronze sources are now active and fan out in parallel under `init_run`, each
carrying one enterprise domain's payload:
`bronze_payments_file` (Auto Loader file drop — the retained payments-practice file
path, kept as one domain, plus marketing/campaign exports),
`bronze_jdbc` (Oracle real-estate PMS + SQL Server treasury/risk, For-Each),
`bronze_customer_cdc` (Debezium customer/guest master → SCD2), and the
**`bronze_rest_api`** (hospitality bookings + FX, [REST_INGEST](REST_INGEST.md)),
**`bronze_sftp`** (entertainment ticketing/footfall, [SFTP_INGEST](SFTP_INGEST.md)), and
**`bronze_salesforce`** (enterprise CRM, [SALESFORCE_INGEST](SALESFORCE_INGEST.md)).

`bronze_validation_gate` depends only on the **required** sources
(`payments_file`, `customer_cdc`) — the REST/SFTP/Salesforce tasks are not gate
dependencies, so a failure in one of them does **not** block the gate or
downstream. To make a source blocking, add it to the gate's `required_sources`
policy (then a FAILED/missing load fails the gate). Each source still records its
own `table_load_status` row, so a non-blocking failure is visible in monitoring
and alerts even when the run proceeds.

Both return `(passed, reasons)`; reasons are logged and (in prod) written to the
control tables for RCA.

## Monitoring / control

`write_status` (run_if `ALL_DONE`) upserts `pipeline_run`, `task_run`, and
`table_load_status` (record counts + failure reason) regardless of outcome —
record builders in `config/control_tables.py`. These feed the dashboards/alerts
described in `docs/DESIGN.md`.

## Separate workflows (production)

Keep long-running streaming and maintenance out of this daily batch job:
`kafka_streaming` (continuous CDC), `maintenance` (OPTIMIZE/VACUUM), and
`backfill_reprocess` (this same graph with `load_mode=backfill`) are separate
Lakeflow jobs.
