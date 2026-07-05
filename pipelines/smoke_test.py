"""
Workspace smoke test — deployment-readiness check that needs NO real source
credentials.

    python pipelines/smoke_test.py            # local / CI
    (Databricks job: investsphere_payments_smoke runs this same file)

Runs the whole orchestration DAG in-process over **seed + in-memory** sources
(the reference InMemory* clients stand in for Oracle / SFTP / Salesforce — i.e.
external calls are disabled), then asserts the deployment is wired correctly:

  * init task runs,
  * Bronze sample tasks run,
  * the Bronze + Silver gates work (gate outcome True, downstream proceeds),
  * the dbt task is callable,
  * governance validation runs (real validate_all -> 0 violations),
  * monitoring writes the control rows (pipeline_run / task_run / table_load_status).

Exits 0 on success, 1 on failure. Prints a JSON summary; never prints secrets.
"""
from __future__ import annotations

import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))
# allow `import job_params` whether run as a script (Databricks) or imported as
# pipelines.smoke_test (tests): put the pipelines dir on the path too.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from job_params import get_param                                   # noqa: E402
from payments_platform.config.audit import RunContext             # noqa: E402
from payments_platform.bronze import jdbc_ingest as J             # noqa: E402
from payments_platform.bronze import rest_ingest as R             # noqa: E402
from payments_platform.bronze import sftp_ingest as S             # noqa: E402
from payments_platform.bronze import salesforce_ingest as SF      # noqa: E402
from payments_platform.bronze import source_config as SC          # noqa: E402
from payments_platform.orchestration import tasks, dag            # noqa: E402
from payments_platform.orchestration.runner import SUCCESS        # noqa: E402
from payments_platform.monitoring.instrument import run_monitored_dag  # noqa: E402

REST_CONFIG = os.path.join(_ROOT, "seeds", "rest", "api_config.json")
SFTP_CONFIG = os.path.join(_ROOT, "seeds", "sftp", "file_config.json")
SFDC_CONFIG = os.path.join(_ROOT, "seeds", "salesforce", "object_config.json")


class SmokeTestFailure(Exception):
    pass


def _in_memory_clients():
    """Reference clients with seed/sample data — disabled external calls."""
    jdbc = J.InMemoryJdbcSource({
        "customers": [{"customer_id": 1, "name": "A", "last_updated_date": "2026-06-30"}],
        "transactions": [{"transaction_id": 9, "amt": 5, "last_updated_date": "2026-06-30"}],
        "accounts": [{"account_id": "AC1", "modified_at": "2026-06-30"}]})
    rest = R.InMemoryRestApi({
        "/v1/fx/rates": [{"rate_id": 1, "pair": "USD/AED", "rate": 3.67,
                          "updated_at": "2026-06-30T08:00:00"}],
        "/v1/merchants": [{"merchant_id": "M1", "name": "Acme",
                           "updated_at": "2026-06-30T08:00:00"}]})
    sftp = S.InMemorySftp({"settlement_2026-06-30.csv":
        "settlement_id,merchant_id,amount,currency,settlement_date\n"
        "S1,M1,100,AED,2026-06-30\n"})
    sfdc = SF.InMemorySalesforce({
        "Account": [{"Id": "001A", "Name": "Acme", "IsDeleted": False,
                     "SystemModstamp": "2026-06-30T10:00:00"}],
        "Contact": [{"Id": "003A", "LastModifiedDate": "2026-06-30T10:00:00",
                     "IsDeleted": True}],
        "Opportunity": [{"Id": "006A", "SystemModstamp": "2026-06-30T10:00:00",
                         "IsDeleted": False}]})
    return jdbc, rest, sftp, sfdc


def run(run_id="smoke-local", env="dev", catalog="investsphere"):
    """Run the smoke test; return a summary dict. Raises SmokeTestFailure on any
    failed check."""
    ctx = RunContext(run_id=run_id, batch_id="smoke-b1",
                     source_system="investsphere_payments",
                     ingestion_timestamp="2026-06-30T18:00:00", environment=env)
    jdbc, rest, sftp, sfdc = _in_memory_clients()
    handlers = tasks.build_handlers(
        ctx, jdbc_source=jdbc, jdbc_configs=SC.load_source_config(
            os.path.join(_ROOT, "seeds", "jdbc", "source_config.json")),
        rest_client=rest, rest_configs=SC.load_configs(REST_CONFIG, SC.REST_REQUIRED_KEYS),
        sftp_client=sftp, sftp_configs=SC.load_configs(SFTP_CONFIG, SC.SFTP_REQUIRED_KEYS),
        sftp_expected_date="2026-06-30",
        sfdc_client=sfdc, sfdc_configs=SC.load_configs(SFDC_CONFIG, SC.SALESFORCE_REQUIRED_KEYS))

    params = {"env": env, "catalog": catalog, "run_date": "2026-06-30",
              "run_id": run_id, "load_mode": "incremental",
              # adversarial seed quarantines a lot; relax so the green path runs
              "silver_policy": {"max_quarantine_rate_pct": 80.0}}

    results, monitor = run_monitored_dag(
        handlers, params, ctx, "investsphere_payments_daily_e2e",
        spike_threshold_pct=90.0)

    checks = {}

    # 1. init task runs
    checks["init_run"] = results["init_run"].status == SUCCESS
    # 2. Bronze sample tasks run
    checks["bronze_tasks_run"] = all(
        results[k].status == SUCCESS for k in (
            "bronze_payments_file", "bronze_jdbc", "bronze_customer_cdc",
            "bronze_rest_api", "bronze_sftp", "bronze_salesforce"))
    # 3. gates work (ran + passed, downstream proceeded)
    checks["bronze_gate_works"] = (
        results["bronze_validation_gate"].outcome is True
        and results["silver_payments"].status == SUCCESS)
    checks["silver_gate_works"] = (
        results["silver_dq_gate"].outcome is True
        and results["dbt_build"].status == SUCCESS)
    # 4. dbt task is callable
    checks["dbt_callable"] = (results["dbt_build"].status == SUCCESS
                              and results["dbt_test"].status == SUCCESS)
    # 5. governance validation runs (real validate_all -> SUCCESS)
    checks["governance_runs"] = results["governance_validation"].status == SUCCESS
    # 6. monitoring writes control rows
    checks["monitoring_control_rows"] = (
        len(monitor.rows("pipeline_run")) == 1
        and len(monitor.rows("task_run")) >= 15
        and len(monitor.rows("table_load_status")) >= 5)
    # overall pipeline finalised SUCCESS
    checks["pipeline_success"] = monitor.final_status == SUCCESS

    summary = {
        "run_id": run_id, "env": env, "catalog": catalog,
        "final_status": monitor.final_status,
        "checks": checks,
        "rows": {name: len(rows) for name, rows in monitor.snapshot().items()},
        "passed": all(checks.values()),
    }
    if not summary["passed"]:
        failed = [k for k, ok in checks.items() if not ok]
        raise SmokeTestFailure("smoke checks failed: " + ", ".join(failed))
    return summary


def main():
    run_id = get_param("run_id", "smoke-local")
    env = get_param("env", "dev")
    catalog = get_param("catalog", "investsphere")
    try:
        summary = run(run_id=run_id, env=env, catalog=catalog)
    except SmokeTestFailure as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, indent=2))
        sys.exit(1)
    print(json.dumps(summary, indent=2))
    print("\nSMOKE TEST PASSED — deployment wiring verified (no source credentials used).")


if __name__ == "__main__":
    main()
