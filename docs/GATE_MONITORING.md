# Gate & pipeline monitoring (Databricks SQL)

The gate tasks and the pipeline endpoints persist their outcomes to Delta control
tables (see `src/payments_platform/databricks/gates.py`):

| Table | Written by | Grain |
|---|---|---|
| `silver_control.pipeline_run_audit` | `init_run` (STARTED), the two gates, `write_status` (SUCCESS/PARTIAL) | one row per gate run + pipeline start/finish |
| `silver_control.dq_results` | the two gates | one row per individual check |
| `silver_control.table_load_status` | the two gates | one row per table the gate observed |

`gate_name` is `bronze` / `silver_dq` for the gates and `pipeline_run` for the
start/finish rows — so `pipeline_run_audit` covers the **whole job**.

> In the queries below, replace `<catalog>` with your catalog (e.g. `investsphere_prod`),
> or make it a Databricks SQL dashboard parameter:
> `IDENTIFIER(:catalog || '.silver_control.pipeline_run_audit')`.

## Dashboard widgets

**1. Latest pipeline run status** (counter / table)
```sql
SELECT run_id, task_name, gate_status, message, check_timestamp
FROM <catalog>.silver_control.pipeline_run_audit
WHERE gate_name = 'pipeline_run'
ORDER BY check_timestamp DESC
LIMIT 10;
```

**2. Bronze gate pass/fail** (counter)
```sql
SELECT gate_status, gate_passed, records_checked, check_timestamp
FROM <catalog>.silver_control.pipeline_run_audit
WHERE gate_name = 'bronze'
ORDER BY check_timestamp DESC
LIMIT 1;
```

**3. Silver DQ gate pass/fail** (counter)
```sql
SELECT gate_status, gate_passed, records_checked, quarantine_rate_pct, check_timestamp
FROM <catalog>.silver_control.pipeline_run_audit
WHERE gate_name = 'silver_dq'
ORDER BY check_timestamp DESC
LIMIT 1;
```

**4. Quarantine rate trend** (line chart: x=`check_timestamp`, y=`quarantine_rate_pct`)
```sql
SELECT check_timestamp, run_id, quarantine_rate_pct
FROM <catalog>.silver_control.pipeline_run_audit
WHERE gate_name = 'silver_dq'
ORDER BY check_timestamp;
```

**5. Failed checks by run** (table)
```sql
SELECT run_id, gate_name, check_name, detail, check_timestamp
FROM <catalog>.silver_control.dq_results
WHERE passed = false
ORDER BY check_timestamp DESC, gate_name, check_name;
```

**6. Table load status by source** (bar / table — latest run)
```sql
SELECT task_name, table_name, source_key, records, status
FROM <catalog>.silver_control.table_load_status
WHERE run_id = (
    SELECT run_id FROM <catalog>.silver_control.pipeline_run_audit
    ORDER BY check_timestamp DESC LIMIT 1)
ORDER BY source_key;
```

## Suggested alerts (Databricks SQL Alerts)

Each query returns a single value; set the alert condition as shown.

**A. A gate failed** — *alert when `failed_gates` > 0*
```sql
SELECT count(*) AS failed_gates
FROM <catalog>.silver_control.pipeline_run_audit
WHERE gate_name IN ('bronze', 'silver_dq')
  AND gate_passed = false
  AND check_timestamp >= current_timestamp() - INTERVAL 1 DAY;
```

**B. Quarantine rate over threshold** — *alert when `max_quarantine_rate_pct` > 30*
```sql
SELECT COALESCE(max(quarantine_rate_pct), 0) AS max_quarantine_rate_pct
FROM <catalog>.silver_control.pipeline_run_audit
WHERE gate_name = 'silver_dq'
  AND check_timestamp >= current_timestamp() - INTERVAL 1 DAY;
```

**C. A required source loaded 0 rows** — *alert when `empty_required_sources` > 0*
```sql
SELECT count(*) AS empty_required_sources
FROM <catalog>.silver_control.table_load_status
WHERE source_key IN ('payments_file', 'customer_cdc')
  AND records = 0
  AND run_id = (
      SELECT run_id FROM <catalog>.silver_control.pipeline_run_audit
      WHERE gate_name = 'bronze' ORDER BY check_timestamp DESC LIMIT 1);
```

The thresholds mirror the gate policies in `gates.py`
(`max_quarantine_rate_pct = 30`, `required_sources = [payments_file, customer_cdc]`) —
keep them in sync when you tune a gate.
