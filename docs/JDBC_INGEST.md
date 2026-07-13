# Metadata-driven JDBC ingestor (Oracle / SQL Server → Bronze)

> This project evolved from a payments-practice foundation into an enterprise business AI decision platform. The original ingestion and lakehouse patterns were preserved and generalized across enterprise domains.

One reusable ingestor drives many tables from a **config table**, with watermark
incremental loads, backfill, retries, duplicate-PK handling and audit columns.
Two source systems are onboarded through this one pattern: the **real-estate
property-management system (Oracle)** and the **investment / treasury
system (SQL Server)**.
The DB is abstracted behind `source.read(source_table, predicate)`, so the same
logic runs against an in-memory fake (the smoke test) or Spark JDBC in Databricks.

## Execution

The incremental logic (full seed → incremental → backfill, watermark, dup-PK) lives in
`payments_platform.bronze.jdbc_ingest` and is imported by the Databricks Bronze task
(`pipelines/dag_task.py`, source `bronze_jdbc`) — a Spark JDBC read that appends the
Bronze Delta table and advances the watermark control row.

## Source config (`silver_control.source_config`)

| column | meaning |
|---|---|
| source_system | oracle / sqlserver |
| source_table | table to extract |
| target_bronze_table | Bronze Delta target |
| primary_key | key for duplicate handling (string or list) |
| watermark_column | incremental column (e.g. `last_updated_date`) |
| load_type | `full` or `incremental` |
| enabled | skip when false |

Reference seed: `seeds/jdbc/source_config.json`; loader `bronze/source_config.py`.

### Onboarded tables

| Source system | Bronze table | Load type | Notes |
|---|---|---|---|
| Oracle (real-estate PMS) | `bronze.oracle_properties` | full | small reference master |
| Oracle (real-estate PMS) | `bronze.oracle_leases` | incremental | watermark `last_updated_date` |
| Oracle (real-estate PMS) | `bronze.oracle_occupancy_daily` | incremental | **large fact** — tuned `fetchsize` + partitioned read |
| Oracle (real-estate PMS) | `bronze.oracle_maintenance_orders` | incremental | watermark `last_updated_date` |
| SQL Server (investment/treasury) | `bronze.sqlserver_assets` | full | small reference master |
| SQL Server (investment/treasury) | `bronze.sqlserver_asset_performance` | incremental | **large fact** — tuned `fetchsize` + partitioned read |
| SQL Server (investment/treasury) | `bronze.sqlserver_risk_exposure` | incremental | watermark `as_of_date` |
| SQL Server (investment/treasury) | `bronze.sqlserver_cashflow` | incremental | watermark `value_date` |

The high-volume daily facts (`oracle_occupancy_daily`, `sqlserver_asset_performance`)
carry `fetchsize`, `partition_column`, `num_partitions`, and `lower_bound/upper_bound`
in their config so the JDBC read is a **bounded parallel read on the numeric PK** —
the rest use plain watermark increments.

## Behaviour

- **First load**: `full`, or `incremental` with no prior watermark → seeds as a
  full load, then sets the watermark to the batch max.
- **Incremental**: reads `watermark_column > last_watermark` (strictly greater),
  advances the watermark **only after a successful read**.
- **No new data**: 0 rows, watermark unchanged.
- **Failure**: read is retried `max_retries` times; if it still fails an
  `IngestionError` is raised and the **watermark is not advanced**.
- **Backfill**: reads an inclusive `start_date..end_date` window and **does not
  move the forward watermark** (so a backfill can't make the daily job skip rows).
- **Duplicate PK**: deduped keeping the row with the greatest watermark.
- **Audit columns**: `source_system, source_table, ingestion_timestamp,
  batch_id, run_id, record_hash, source_extract_timestamp` on every Bronze row.
- **For-Each runner**: loops configs, skips disabled tables, records a per-table
  failure and continues with the rest (one bad table never blocks the others).

## Mapping to Spark JDBC in Databricks

The in-memory source is the only thing that changes. The predicate this ingestor
computes (`column`, `low`, `high`, `mode`) becomes a **pushed-down WHERE** and a
**partitioned read**:

```python
# incremental: predicate {column: last_updated_date, low: <wm>, mode: incremental}
df = (spark.read.format("jdbc")
      .option("url", jdbc_url)                 # Oracle / SQL Server
      .option("dbtable",
              f"(SELECT * FROM {src} WHERE {wm_col} > '{last_wm}') t")  # pushdown
      .option("user", dbutils.secrets.get(scope, "jdbc-user"))
      .option("password", dbutils.secrets.get(scope, "jdbc-pass"))
      # partitioned parallel read for large tables:
      .option("partitionColumn", pk).option("numPartitions", 16)
      .option("lowerBound", lo).option("upperBound", hi)
      .load())

bronze = add_audit(df, config, ctx)            # same audit columns
bronze.dropDuplicates([pk])                     # duplicate-PK handling
bronze.write.format("delta").mode("append").saveAsTable(target_bronze_table)

# advance watermark ONLY after the write succeeds:
new_wm = bronze.agg(max(wm_col)).collect()[0][0]
upsert_watermark(source_table, new_wm)          # silver_control.ingestion_watermark
```

Key parity points:

| Reference | Databricks |
|---|---|
| `source.read(table, predicate)` | Spark JDBC with WHERE pushdown |
| `low/high` bounds | `lowerBound/upperBound` + partitioned read |
| `WatermarkStore` | `silver_control.ingestion_watermark` Delta table |
| config list | `silver_control.source_config` Delta table |
| `run_ingestion` loop | a **For-Each task** over the config in a Lakeflow Job |
| `_read_with_retry` | task `max_retries` + transient-error retry |
| secrets (not in code) | `dbutils.secrets.get` over a Key Vault-backed scope |

**Don't over-parallelise**: too many `numPartitions` opens too many JDBC
connections and can overload Oracle/SQL Server — size to the source, not the
cluster. Use a self-hosted gateway/private endpoint for the on-prem SQL Server.
