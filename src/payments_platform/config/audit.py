"""
Run context + Bronze audit columns.

Every Bronze row carries technical lineage so the lakehouse is fully traceable
and replayable. The timestamps are passed IN via :class:`RunContext` (never read
from the wall clock here) so ingestion is deterministic and unit-testable.

Common Bronze columns added (per the project spec):
    source_system, source_file_name, source_file_path, ingestion_timestamp,
    batch_id, run_id, record_hash, operation_type, source_extract_timestamp,
    _corrupt_record
"""
from __future__ import annotations

from payments_platform.common.hashing import record_hash

# Technical columns Bronze attaches to every row. Kept as a tuple so callers and
# tests can assert the contract.
BRONZE_AUDIT_COLUMNS = (
    "source_system",
    "source_file_name",
    "source_file_path",
    "ingestion_timestamp",
    "batch_id",
    "run_id",
    "record_hash",
    "operation_type",
    "source_extract_timestamp",
    "_corrupt_record",
)


class RunContext:
    """Carries the identifiers + timestamps for one pipeline run.

    Passing these explicitly (instead of calling now()) keeps Bronze ingestion
    deterministic, so tests and replays produce identical rows.
    """

    def __init__(
        self,
        run_id,
        batch_id,
        source_system,
        ingestion_timestamp,
        source_extract_timestamp=None,
        environment="dev",
    ):
        self.run_id = run_id
        self.batch_id = batch_id
        self.source_system = source_system
        self.ingestion_timestamp = ingestion_timestamp
        # When the source produced the extract (file mtime / DB extract time /
        # CDC source ts). Falls back to ingestion time if the source has none.
        self.source_extract_timestamp = (
            source_extract_timestamp or ingestion_timestamp
        )
        self.environment = environment


def add_audit_columns(
    row,
    ctx,
    business_columns,
    source_file_name=None,
    source_file_path=None,
    operation_type=None,
    corrupt_record=None,
):
    """Return a copy of ``row`` with the Bronze audit columns populated.

    Args:
        row:              business payload (a dict).
        ctx:              the :class:`RunContext`.
        business_columns: columns to include in ``record_hash`` (the payload
                          columns, not the audit columns).
        operation_type:   INSERT/UPDATE/DELETE/READ for CDC; None -> "INSERT"
                          for plain file/append loads.
        corrupt_record:   raw text of a row that failed to parse; None if clean.
    """
    enriched = dict(row)
    enriched["source_system"] = ctx.source_system
    enriched["source_file_name"] = source_file_name
    enriched["source_file_path"] = source_file_path
    enriched["ingestion_timestamp"] = ctx.ingestion_timestamp
    enriched["batch_id"] = ctx.batch_id
    enriched["run_id"] = ctx.run_id
    enriched["operation_type"] = operation_type or "INSERT"
    enriched["source_extract_timestamp"] = ctx.source_extract_timestamp
    enriched["_corrupt_record"] = corrupt_record
    # hash only the business payload, so audit columns don't affect change-detection
    enriched["record_hash"] = record_hash(row, business_columns)
    return enriched
