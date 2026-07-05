"""
Control / audit table records.

These build the rows written to ``silver_control.*`` tables that drive
orchestration, restartability, SLA reporting and RCA (per the spec's
"control tables for audit and restart" section). Pure dict builders so they are
trivially unit-tested; the job writes them to Delta.
"""
from __future__ import annotations

# lifecycle statuses
SUCCESS = "SUCCESS"
FAILED = "FAILED"
RUNNING = "RUNNING"
STARTED = "STARTED"      # pipeline_run opened, not yet finished
PARTIAL = "PARTIAL"      # finished but a gate blocked part of the flow
SKIPPED = "SKIPPED"      # task not run (upstream blocked)


def table_load_status(
    ctx,
    source_table,
    target_table,
    load_type,
    records_read,
    records_written,
    records_rejected,
    watermark_value=None,
    status=SUCCESS,
    error_message=None,
    corrupt_record_count=0,
    start_time=None,
    end_time=None,
):
    """One row for ``silver_control.table_load_status``."""
    return {
        "run_id": ctx.run_id,
        "source_system": ctx.source_system,
        "source_table": source_table,
        "target_table": target_table,
        "load_type": load_type,
        "records_read": records_read,
        "records_written": records_written,
        "records_rejected": records_rejected,
        "corrupt_record_count": corrupt_record_count,
        "watermark_value": watermark_value,
        "status": status,
        "error_message": error_message,
        "start_time": start_time or ctx.ingestion_timestamp,
        "end_time": end_time or ctx.ingestion_timestamp,
    }


def task_run(ctx, task_key, status=SUCCESS, records_written=0,
             error_message=None, phase=None, attempt=1,
             start_time=None, end_time=None):
    """One row for ``silver_control.task_run`` (per-task audit).

    ``attempt`` records retries (Lakeflow task retry number); a retried task
    writes one row per attempt.
    """
    return {
        "run_id": ctx.run_id,
        "task_key": task_key,
        "phase": phase,
        "status": status,
        "attempt": attempt,
        "records_written": records_written,
        "start_time": start_time or ctx.ingestion_timestamp,
        "end_time": end_time or ctx.ingestion_timestamp,
        "error_message": error_message,
    }


def pipeline_run(ctx, job_name, status=SUCCESS, error_message=None,
                 triggered_by="service_principal", start_time=None, end_time=None):
    """One row for ``silver_control.pipeline_run``.

    A run is opened with ``status=STARTED`` (``end_time`` left None) and closed by
    re-stamping the same row with the final SUCCESS/FAILED/PARTIAL + ``end_time``.
    """
    return {
        "run_id": ctx.run_id,
        "run_date": ctx.ingestion_timestamp[:10],  # YYYY-MM-DD
        "environment": ctx.environment,
        "job_name": job_name,
        "status": status,
        "start_time": start_time or ctx.ingestion_timestamp,
        "end_time": end_time,
        "triggered_by": triggered_by,
        "error_message": error_message,
    }
