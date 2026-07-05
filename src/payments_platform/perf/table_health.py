"""
Table-health checks (reference).

Simulates Delta file layout from row counts and flags the classic maintenance
problems: too many small files (OPTIMIZE), retained tombstones/old files
(VACUUM), and stale freshness (SLA breach). Pure functions returning structured
findings + recommendations.

In production these map to ``DESCRIBE DETAIL`` (numFiles / sizeInBytes),
``system.storage`` / file listings, OPTIMIZE/VACUUM history, and Lakehouse
Monitoring — see docs/PERFORMANCE_COST.md.
"""
from __future__ import annotations

from payments_platform.monitoring import models as M

TARGET_FILE_MB = 128.0          # Delta's preferred file size
SMALL_FILE_AVG_MB = 32.0        # below this average -> small-file problem
AVG_ROW_BYTES = 256             # reference row width for the simulation
MANY_FILES = 1000               # file count above which OPTIMIZE is recommended


def file_count_simulation(num_rows, rows_per_file=None, avg_row_bytes=AVG_ROW_BYTES,
                          target_file_mb=TARGET_FILE_MB):
    """Estimate file layout for a table of ``num_rows``.

    If ``rows_per_file`` is given (e.g. lots of tiny streaming commits), it is
    used directly; otherwise rows are packed into ``target_file_mb`` files.
    """
    total_mb = num_rows * avg_row_bytes / (1024.0 * 1024.0)
    if rows_per_file:
        num_files = max(1, -(-num_rows // rows_per_file))   # ceil
    else:
        rows_per_target = max(1, int(target_file_mb * 1024 * 1024 / avg_row_bytes))
        num_files = max(1, -(-num_rows // rows_per_target))
    avg_file_mb = round(total_mb / num_files, 4) if num_files else 0.0
    return {"num_rows": num_rows, "num_files": num_files,
            "total_mb": round(total_mb, 4), "avg_file_mb": avg_file_mb}


def small_file_warning(layout, min_avg_mb=SMALL_FILE_AVG_MB):
    """True when the average file is smaller than ``min_avg_mb`` (small-file
    problem — hurts read planning and scan throughput)."""
    return (layout["num_files"] > 1
            and layout["avg_file_mb"] < min_avg_mb)


def optimize_recommendation(layout, min_avg_mb=SMALL_FILE_AVG_MB):
    """Recommend OPTIMIZE (compaction) when there are many small files."""
    if small_file_warning(layout, min_avg_mb) or layout["num_files"] > MANY_FILES:
        return {"action": "OPTIMIZE",
                "reason": "%d files, avg %.1f MB (< %.0f MB target)" % (
                    layout["num_files"], layout["avg_file_mb"], min_avg_mb),
                "recommended": True}
    return {"action": "OPTIMIZE", "recommended": False}


def vacuum_recommendation(tombstoned_files=0, hours_since_last_vacuum=None,
                          retention_hours=168):
    """Recommend VACUUM when tombstoned/old files have accumulated and no recent
    VACUUM has run (default retention 7 days)."""
    overdue = (hours_since_last_vacuum is not None
               and hours_since_last_vacuum > retention_hours)
    recommend = tombstoned_files > 0 and (overdue or hours_since_last_vacuum is None)
    return {"action": "VACUUM", "recommended": recommend,
            "tombstoned_files": tombstoned_files,
            "reason": ("%d tombstoned files; last VACUUM %s" % (
                tombstoned_files,
                "never" if hours_since_last_vacuum is None
                else "%dh ago" % hours_since_last_vacuum)) if recommend else None}


def freshness_warning(ctx, table, last_loaded_at, as_of, sla_minutes):
    """Reuse the monitoring freshness model; return the row + a breach flag."""
    row = M.table_freshness(ctx, table, last_loaded_at, as_of, sla_minutes)
    return {"table": table, "lag_minutes": row["lag_minutes"],
            "sla_minutes": sla_minutes, "stale": row["sla_breached"], "row": row}


def assess_table(ctx, table, num_rows, rows_per_file=None, last_loaded_at=None,
                 as_of=None, sla_minutes=180, tombstoned_files=0,
                 hours_since_last_vacuum=None):
    """Full health assessment for one table: layout + OPTIMIZE/VACUUM/freshness."""
    layout = file_count_simulation(num_rows, rows_per_file=rows_per_file)
    findings = {
        "table": table,
        "layout": layout,
        "small_files": small_file_warning(layout),
        "optimize": optimize_recommendation(layout),
        "vacuum": vacuum_recommendation(tombstoned_files, hours_since_last_vacuum),
    }
    if last_loaded_at and as_of:
        findings["freshness"] = freshness_warning(
            ctx, table, last_loaded_at, as_of, sla_minutes)
    return findings
