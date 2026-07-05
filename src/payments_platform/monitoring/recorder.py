"""
``RunMonitor`` — the Python reference implementation of the monitoring API.

It accumulates monitoring rows in memory (one list per model); in production the
same calls upsert Delta tables in ``silver_control.*`` / ``monitoring.*``. Every
timestamp comes from the :class:`RunContext`, so a run records identically on
every replay.

Lifecycle:
    m = RunMonitor(ctx, "investsphere_payments_daily_e2e")
    m.start_pipeline()                       # pipeline_run STARTED
    m.record_task("init_run", SUCCESS, ...)  # task_run per task (retries -> attempt N)
    m.record_load_status(...)                # table_load_status / row counts / rejects
    m.record_dq(...); m.record_quarantine(...); m.record_freshness(...)
    m.record_dbt(...); m.record_security(...); m.record_cost(...)
    m.finalize_pipeline(SUCCESS|FAILED|PARTIAL)
"""
from __future__ import annotations

from payments_platform.monitoring import models as M
from payments_platform.monitoring.models import SUCCESS, FAILED, STARTED, PARTIAL


class RunMonitor:
    def __init__(self, ctx, job_name, triggered_by="service_principal"):
        self.ctx = ctx
        self.job_name = job_name
        self.triggered_by = triggered_by
        self.final_status = None
        # one list of rows per model
        self.tables = {name: [] for name in M.MODEL_TABLES}

    # ---- 2a. start a pipeline run ----------------------------------------
    def start_pipeline(self):
        """Open the run: a single pipeline_run row with status STARTED."""
        row = M.pipeline_run(self.ctx, self.job_name, status=STARTED,
                             triggered_by=self.triggered_by)
        self.tables["pipeline_run"].append(row)
        return row

    # ---- 2b. update task status (records retries) ------------------------
    def record_task(self, task_key, status=SUCCESS, phase=None,
                    records_written=0, error_message=None, attempt=None):
        """Write a task_run row. ``attempt`` auto-increments per task_key so a
        retried task produces one row per attempt (attempt 1, 2, ...)."""
        if attempt is None:
            attempt = self.task_attempts(task_key) + 1
        row = M.task_run(self.ctx, task_key, status=status, phase=phase,
                        records_written=records_written,
                        error_message=error_message, attempt=attempt)
        self.tables["task_run"].append(row)
        return row

    def task_attempts(self, task_key):
        return sum(1 for r in self.tables["task_run"] if r["task_key"] == task_key)

    # ---- 2c. record row counts / 2d. failed / corrupt / rejected ---------
    def record_load_status(self, source_table, target_table, load_type,
                           records_read, records_written, records_rejected=0,
                           corrupt_record_count=0, status=SUCCESS,
                           watermark_value=None, error_message=None):
        """table_load_status row: row counts plus rejected/corrupt counts."""
        row = M.table_load_status(
            self.ctx, source_table, target_table, load_type,
            records_read=records_read, records_written=records_written,
            records_rejected=records_rejected,
            corrupt_record_count=corrupt_record_count,
            watermark_value=watermark_value, status=status,
            error_message=error_message)
        self.tables["table_load_status"].append(row)
        return row

    # ---- 2e. record a DQ result summary ----------------------------------
    def record_dq(self, entity, rule_name, severity, records_evaluated,
                  records_failed, stage="silver", action="QUARANTINE",
                  result=None, detail=None):
        row = M.dq_results(self.ctx, entity, rule_name, severity,
                          records_evaluated, records_failed, stage=stage,
                          action=action, result=result, detail=detail)
        self.tables["dq_results"].append(row)
        return row

    def record_quarantine(self, entity, source_table, records_total,
                          records_quarantined, spike_threshold_pct=None):
        row = M.quarantine_summary(self.ctx, entity, source_table,
                                  records_total, records_quarantined,
                                  spike_threshold_pct=spike_threshold_pct)
        self.tables["quarantine_summary"].append(row)
        return row

    # ---- 2f. record table freshness --------------------------------------
    def record_freshness(self, table, last_loaded_at, as_of, sla_minutes):
        row = M.table_freshness(self.ctx, table, last_loaded_at, as_of,
                               sla_minutes)
        self.tables["table_freshness"].append(row)
        return row

    # ---- 2g. record a dbt build/test result ------------------------------
    def record_dbt(self, command, status, models_run=0, tests_run=0,
                   tests_passed=0, tests_failed=0, detail=None):
        row = M.dbt_results(self.ctx, command, status, models_run=models_run,
                          tests_run=tests_run, tests_passed=tests_passed,
                          tests_failed=tests_failed, detail=detail)
        self.tables["dbt_results"].append(row)
        return row

    # ---- 2h. record a governance/security validation result --------------
    def record_security(self, check_name, violations, severity="HIGH",
                        detail=None):
        row = M.security_events(self.ctx, check_name, violations,
                              severity=severity, detail=detail)
        self.tables["security_events"].append(row)
        return row

    # ---- cost ------------------------------------------------------------
    def record_cost(self, warehouse, dbus, cost_usd, budget_usd=None,
                    threshold_pct=80.0):
        row = M.cost_summary(self.ctx, warehouse, dbus, cost_usd,
                           budget_usd=budget_usd, threshold_pct=threshold_pct)
        self.tables["cost_summary"].append(row)
        return row

    # ---- 2i. mark final pipeline status ----------------------------------
    def finalize_pipeline(self, status, reason=None, ended_at=None):
        """Close the run: re-stamp the STARTED pipeline_run row with the final
        SUCCESS / FAILED / PARTIAL and an end_time. Idempotent per run."""
        if status not in (SUCCESS, FAILED, PARTIAL):
            raise ValueError("final status must be SUCCESS/FAILED/PARTIAL")
        self.final_status = status
        end = ended_at or self.ctx.ingestion_timestamp
        rows = self.tables["pipeline_run"]
        if not rows:                       # finalize without an explicit start
            self.start_pipeline()
        row = rows[0]
        row["status"] = status
        row["end_time"] = end
        if reason is not None:
            row["error_message"] = reason
        return row

    # ---- read side -------------------------------------------------------
    def rows(self, model):
        return list(self.tables[model])

    def snapshot(self):
        """An immutable view of every model's rows (for alerts/dashboards)."""
        return {name: list(rows) for name, rows in self.tables.items()}
