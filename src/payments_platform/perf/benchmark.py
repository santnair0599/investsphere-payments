"""
Performance benchmark runner.

Times each medallion stage over synthetic data and writes the timings/counts into
a :class:`RunMonitor` (table_load_status / quarantine_summary / dbt_results /
cost_summary), so a benchmark run is observable exactly like a real run.

Timings use ``time.perf_counter`` (a monotonic clock) — they are *informational*;
tests assert that rows are written and that scalability behaviour is correct, never
specific wall-clock numbers. The Databricks equivalents (query history, job
timelines, billing) are in docs/PERFORMANCE_COST.md.
"""
from __future__ import annotations

from time import perf_counter

from payments_platform.config.audit import RunContext
from payments_platform.bronze import file_ingest, jdbc_ingest as J
from payments_platform.bronze import rest_ingest as R, sftp_ingest as S
from payments_platform.bronze import salesforce_ingest as SF
from payments_platform.silver import parse, dedup, quarantine, dq
from payments_platform.silver.cdc_apply import apply_scd2
from payments_platform.orchestration import tasks
from payments_platform.orchestration.runner import SUCCESS
from payments_platform.monitoring.recorder import RunMonitor
from payments_platform.monitoring.instrument import run_monitored_dag
from payments_platform.perf import synth, cost

# how many JDBC source tables to configure per size (scalability fan-out)
JDBC_TABLES = {"small": 5, "medium": 20, "large": 50}


def timed(fn, *args, **kwargs):
    """Run ``fn`` and return (result, elapsed_ms)."""
    start = perf_counter()
    result = fn(*args, **kwargs)
    return result, (perf_counter() - start) * 1000.0


# --------------------------------------------------------------------------- #
# scalability helper: many metadata-driven JDBC configs
# --------------------------------------------------------------------------- #
def make_jdbc_configs(n_tables, rows_per_table=20):
    """Build ``n_tables`` JDBC source-configs + an in-memory source (For-Each)."""
    configs, tables = [], {}
    for t in range(n_tables):
        name = "tbl_%03d" % t
        configs.append({
            "source_system": "oracle", "source_table": name,
            "target_bronze_table": "bronze.%s" % name, "primary_key": "id",
            "watermark_column": "updated_at", "load_type": "full", "enabled": True})
        tables[name] = [{"id": i, "updated_at": "2026-06-30", "val": i}
                        for i in range(rows_per_table)]
    return configs, J.InMemoryJdbcSource(tables)


# --------------------------------------------------------------------------- #
# per-stage benchmarks
# --------------------------------------------------------------------------- #
def bench_bronze_file(gen, ctx):
    clean, corrupt = file_ingest.parse_csv(gen["csv"], gen["columns"])
    bronze, ms = timed(file_ingest.ingest_records, clean, corrupt, ctx,
                       gen["columns"], "synth_payments.csv", "/landing/synth.csv")
    return {"stage": "bronze_file_ingest", "rows": len(bronze),
            "corrupt": len(corrupt), "elapsed_ms": ms}


def bench_jdbc_incremental(configs, source, ctx):
    res, ms = timed(J.run_ingestion, configs, source, J.WatermarkStore(), ctx)
    written = sum(r.get("records_written", 0) for r in res)
    return {"stage": "jdbc_incremental", "tables": len(configs),
            "rows": written, "elapsed_ms": ms, "results": res}


def bench_cdc_scd2(events, ctx):
    scd2, ms = timed(apply_scd2, [], events, "customer_id", synth.CUSTOMER_TRACKED)
    current = [r for r in scd2 if r["is_current"]]
    return {"stage": "cdc_scd2_apply", "events": len(events),
            "current_rows": len(current), "elapsed_ms": ms}


def bench_silver_dq(gen, ctx):
    parsed = [parse.parse_payment(r) for r in gen["rows"]]

    def work():
        deduped = dedup.dedup_latest(parsed, ["payment_id"], "ingestion_timestamp")
        return quarantine.split(deduped, dq.PAYMENT_RULES, ctx, "bronze.payments")

    (valid, quarantined), ms = timed(work)
    return {"stage": "silver_dq_quarantine", "valid": len(valid),
            "quarantined": len(quarantined), "elapsed_ms": ms}


def bench_dbt_gold_sim(gen):
    rows = gen["rows"]

    def build():
        agg = {}
        for r in rows:
            amt = parse.to_decimal(r.get("amount")) or 0
            key = (r.get("customer_id"), r.get("currency"))
            agg[key] = agg.get(key, 0) + amt
        return agg

    agg, ms = timed(build)
    return {"stage": "dbt_gold_build_sim", "input_rows": len(rows),
            "gold_rows": len(agg), "elapsed_ms": ms}


def _e2e_handlers(ctx):
    jdbc = J.InMemoryJdbcSource({
        "customers": [{"customer_id": 1, "name": "A", "last_updated_date": "2026-06-30"}]})
    rest = R.InMemoryRestApi({"/v1/fx": [
        {"rate_id": 1, "rate": 3.6, "updated_at": "2026-06-30T08:00:00"}]})
    sftp = S.InMemorySftp({"settlement_2026-06-30.csv":
        "settlement_id,amount,settlement_date\nS1,100,2026-06-30\n"})
    sfdc = SF.InMemorySalesforce({"Account": [
        {"Id": "001A", "IsDeleted": False, "SystemModstamp": "2026-06-30T10:00:00"}]})
    return tasks.build_handlers(
        ctx, jdbc_source=jdbc, jdbc_configs=[{
            "source_system": "oracle", "source_table": "customers",
            "target_bronze_table": "bronze.oracle_customers",
            "primary_key": "customer_id", "watermark_column": "last_updated_date",
            "load_type": "full", "enabled": True}],
        rest_client=rest, rest_configs=[{
            "source_system": "p", "api_name": "fx", "endpoint": "/v1/fx",
            "target_bronze_table": "bronze.fx", "primary_key": "rate_id",
            "load_type": "full", "enabled": True, "request_params": {},
            "auth": {"scheme": "Bearer", "secret_name": "rest_api_token"},
            "pagination": {"type": "page", "size": 50},
            "watermark": {"param": "updated_since", "field": "updated_at"}}],
        sftp_client=sftp, sftp_configs=[{
            "source_system": "vendor",
            "file_pattern": r"settlement_(?P<date>\d{4}-\d{2}-\d{2})\.csv",
            "business_columns": ["settlement_id", "amount", "settlement_date"],
            "target_bronze_table": "bronze.settlements", "enabled": True}],
        sftp_expected_date="2026-06-30",
        sfdc_client=sfdc, sfdc_configs=[{
            "source_system": "salesforce", "source_object": "Account",
            "target_bronze_table": "bronze.sfdc_account", "primary_key": "Id",
            "watermark_column": "SystemModstamp", "load_type": "full",
            "include_deleted": True, "enabled": True}])


def bench_end_to_end(ctx):
    params = {"env": ctx.environment, "catalog": "investsphere",
              "run_date": "2026-06-30", "run_id": ctx.run_id,
              "load_mode": "incremental",
              "silver_policy": {"max_quarantine_rate_pct": 80.0}}

    def run():
        return run_monitored_dag(_e2e_handlers(ctx), params, ctx,
                                 "investsphere_payments_daily_e2e",
                                 spike_threshold_pct=90.0)

    (results, mon), ms = timed(run)
    return {"stage": "end_to_end_dag", "final": mon.final_status,
            "tasks": len(results), "elapsed_ms": ms}


# --------------------------------------------------------------------------- #
# orchestrating runner: time every stage + write monitoring rows
# --------------------------------------------------------------------------- #
def run_benchmarks(sizes=("small",), monitor=None, ctx=None, env="dev",
                   compute_type="serverless_job", budget_usd=None):
    """Run all benchmarks for each size; write monitoring rows. Returns
    (results, monitor)."""
    ctx = ctx or RunContext("bench-run", "bench-b", "investsphere_payments",
                            "2026-06-30T18:00:00", environment=env)
    monitor = monitor or RunMonitor(ctx, "investsphere_payments_benchmarks")
    monitor.start_pipeline()
    results = {}

    for size in sizes:
        gen = synth.generate_payments(size)
        cdc = synth.generate_cdc_events(size)
        configs, source = make_jdbc_configs(JDBC_TABLES.get(size, 5))

        bfile = bench_bronze_file(gen, ctx)
        monitor.record_load_status("synth.payments_csv", "bronze.payments", "batch",
                                   records_read=bfile["rows"] + bfile["corrupt"],
                                   records_written=bfile["rows"],
                                   records_rejected=bfile["corrupt"],
                                   corrupt_record_count=bfile["corrupt"])

        jdbc = bench_jdbc_incremental(configs, source, ctx)
        # one control row PER configured table (scalability: per-table monitoring)
        for r in jdbc["results"]:
            monitor.record_load_status(
                r["source_table"], r.get("target_bronze_table", "bronze." + r["source_table"]),
                "full", records_read=r.get("records_read", 0),
                records_written=r.get("records_written", 0))

        cdc_b = bench_cdc_scd2(cdc["events"], ctx)
        monitor.record_load_status("cdc.customer", "silver.customer_scd2", "cdc",
                                   records_read=cdc["n_events"] + cdc["n_duplicate"],
                                   records_written=cdc_b["current_rows"])

        silver = bench_silver_dq(gen, ctx)
        total = silver["valid"] + silver["quarantined"]
        monitor.record_quarantine("payments", "silver.payments", total,
                                  silver["quarantined"], spike_threshold_pct=50.0)

        gold = bench_dbt_gold_sim(gen)
        monitor.record_dbt("build", SUCCESS, models_run=3,
                          detail="gold build simulation (%d gold rows)" % gold["gold_rows"])

        e2e = bench_end_to_end(ctx)

        # cost rows from the per-stage durations (attributed to DAG tasks)
        durations = {
            "bronze_payments_file": bfile["elapsed_ms"],
            "bronze_jdbc": jdbc["elapsed_ms"],
            "silver_customer_scd2": cdc_b["elapsed_ms"],
            "silver_payments": silver["elapsed_ms"],
            "dbt_build": gold["elapsed_ms"],
        }
        cost_rows = cost.estimate_costs(durations, compute_type=compute_type, env=env)
        cost.record_costs(monitor, cost_rows, budget_usd=budget_usd)

        results[size] = {
            "stages": {s["stage"]: s for s in
                       (bfile, jdbc, cdc_b, silver, gold, e2e)},
            "cost_rows": cost_rows,
            "total_cost_usd": cost.total_cost(cost_rows),
            "data": {"n_clean": gen["n_clean"], "n_duplicate": gen["n_duplicate"],
                     "n_invalid": gen["n_invalid"], "n_corrupt": gen["n_corrupt"],
                     "cdc_events": cdc["n_events"], "cdc_late": cdc["n_late"],
                     "jdbc_tables": len(configs)},
        }

    monitor.finalize_pipeline(SUCCESS)
    return results, monitor
