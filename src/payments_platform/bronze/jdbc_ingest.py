"""
Metadata-driven JDBC ingestor for Oracle / SQL Server -> Bronze (reference).

Covers the spec's "Database tables to Bronze": full vs incremental loads driven
by a **source config table**, watermark handling, backfill, retries, audit
columns, duplicate-PK handling, and a For-Each runner over many tables.

Pure-Python and testable: the database is abstracted behind a `source.read(
source_table, predicate)` call, so tests inject an in-memory source. In
Databricks the same flow maps to a partitioned Spark JDBC read with the predicate
pushed down — see ``docs/JDBC_INGEST.md`` and :class:`SparkJdbcSource` notes.

Watermark + config live in Delta control tables in production
(``silver_control.source_config`` / ``silver_control.ingestion_watermark``); here
they're a config list and an in-memory :class:`WatermarkStore`.
"""
from __future__ import annotations

from payments_platform.common.hashing import record_hash

# audit columns this ingestor guarantees on every Bronze row
JDBC_AUDIT_COLUMNS = (
    "source_system", "source_table", "ingestion_timestamp",
    "batch_id", "run_id", "record_hash", "source_extract_timestamp",
)

FULL = "full"
INCREMENTAL = "incremental"


class IngestionError(Exception):
    """Raised when a table read fails after all retries."""


# ---------------------------------------------------------------------------
# Watermark store (reference; Delta `silver_control.ingestion_watermark` in prod)
# ---------------------------------------------------------------------------
class WatermarkStore:
    def __init__(self, initial=None):
        self._wm = dict(initial or {})

    def get(self, source_table):
        return self._wm.get(source_table)

    def set(self, source_table, value):
        self._wm[source_table] = value

    def snapshot(self):
        return dict(self._wm)


# ---------------------------------------------------------------------------
# Source abstraction
# ---------------------------------------------------------------------------
class InMemoryJdbcSource:
    """Test/reference source. `tables` = {source_table: [row dicts]}.

    ``read`` honours the predicate the way a Spark JDBC pushdown would:
    incremental = strictly greater than the last watermark; backfill = inclusive
    between bounds; full = everything.
    """

    def __init__(self, tables):
        self.tables = tables
        self.read_calls = 0

    def read(self, source_table, predicate):
        self.read_calls += 1
        rows = self.tables.get(source_table, [])
        if predicate is None:
            return [dict(r) for r in rows]
        col = predicate["column"]
        low, high, mode = predicate.get("low"), predicate.get("high"), predicate["mode"]
        out = []
        for r in rows:
            v = r.get(col)
            if v is None:
                continue
            if mode == "backfill":
                if (low is None or v >= low) and (high is None or v <= high):
                    out.append(dict(r))
            else:  # incremental: strictly greater than last watermark
                if (low is None or v > low) and (high is None or v <= high):
                    out.append(dict(r))
        return out


class FailingSource:
    """Source that always raises — used to test retry/failure handling."""

    def __init__(self, exc=RuntimeError("connection reset")):
        self.exc = exc
        self.read_calls = 0

    def read(self, source_table, predicate):
        self.read_calls += 1
        raise self.exc


# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------
def _read_with_retry(source, source_table, predicate, max_retries):
    """Call source.read, retrying transient failures up to max_retries times."""
    attempts = 0
    last_exc = None
    while attempts <= max_retries:
        try:
            return source.read(source_table, predicate)
        except Exception as exc:  # noqa: BLE001 — reference retry-all
            last_exc = exc
            attempts += 1
    raise IngestionError(
        "read failed after %d attempts for %s: %s"
        % (max_retries + 1, source_table, last_exc))


def _dedup_by_pk(rows, primary_key, watermark_column):
    """Keep one row per primary key — the latest by watermark (or last seen)."""
    best = {}
    for row in rows:
        key = tuple(row.get(c) for c in primary_key)
        current = best.get(key)
        if current is None:
            best[key] = row
        elif watermark_column is not None:
            if row.get(watermark_column) >= current.get(watermark_column):
                best[key] = row
        else:
            best[key] = row  # no watermark -> last wins
    return list(best.values())


def _add_audit(row, config, ctx):
    enriched = dict(row)
    enriched["source_system"] = config["source_system"]
    enriched["source_table"] = config["source_table"]
    enriched["ingestion_timestamp"] = ctx.ingestion_timestamp
    enriched["batch_id"] = ctx.batch_id
    enriched["run_id"] = ctx.run_id
    enriched["source_extract_timestamp"] = ctx.source_extract_timestamp
    enriched["record_hash"] = record_hash(row, list(row.keys()))
    return enriched


def _resolve_predicate(config, watermark_store, backfill):
    """Decide load mode + predicate. Returns (mode, predicate)."""
    wm_col = config.get("watermark_column")
    if backfill:
        return "backfill", {"column": wm_col, "low": backfill["start_date"],
                            "high": backfill["end_date"], "mode": "backfill"}
    if config["load_type"] == FULL:
        return "full", None
    # incremental: first run (no watermark) behaves as a full seed load
    last = watermark_store.get(config["source_table"])
    if last is None:
        return "full_seed", None
    return "incremental", {"column": wm_col, "low": last, "high": None,
                           "mode": "incremental"}


def ingest_table(config, source, watermark_store, ctx, backfill=None,
                 max_retries=2):
    """Ingest one configured table into Bronze.

    Watermark advances ONLY after a successful read, and never during backfill
    (so a backfill can't move the forward watermark and skip daily rows).

    Raises:
        IngestionError: if the read fails after retries (watermark untouched).
    """
    pk = config["primary_key"] if isinstance(config["primary_key"], list) \
        else [config["primary_key"]]
    wm_col = config.get("watermark_column")

    mode, predicate = _resolve_predicate(config, watermark_store, backfill)
    rows = _read_with_retry(source, config["source_table"], predicate, max_retries)
    rows = _dedup_by_pk(rows, pk, wm_col)
    bronze = [_add_audit(r, config, ctx) for r in rows]

    # compute + persist watermark (success only, never on backfill)
    new_watermark = watermark_store.get(config["source_table"])
    if wm_col and rows:
        batch_max = max(r[wm_col] for r in rows if r.get(wm_col) is not None)
        if mode != "backfill":
            # advance only forward
            if new_watermark is None or batch_max > new_watermark:
                new_watermark = batch_max
            watermark_store.set(config["source_table"], new_watermark)

    return {
        "source_table": config["source_table"],
        "target_bronze_table": config["target_bronze_table"],
        "status": "SUCCESS",
        "mode": mode,
        "records_read": len(rows),
        "records_written": len(bronze),
        "watermark": watermark_store.get(config["source_table"]),
        "bronze": bronze,
    }


# ---------------------------------------------------------------------------
# For-Each runner
# ---------------------------------------------------------------------------
def run_ingestion(configs, source, watermark_store, ctx, backfill=None,
                  max_retries=2):
    """Loop configured tables (the For-Each task). Disabled tables are skipped;
    a per-table failure is recorded and the run continues with the rest."""
    results = []
    for config in configs:
        if not config.get("enabled", True):
            results.append({"source_table": config["source_table"],
                            "status": "SKIPPED", "records_written": 0})
            continue
        try:
            results.append(ingest_table(
                config, source, watermark_store, ctx, backfill, max_retries))
        except IngestionError as exc:
            results.append({"source_table": config["source_table"],
                            "status": "FAILED", "records_written": 0,
                            "error": str(exc)})
    return results
