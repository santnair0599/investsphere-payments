"""
Salesforce ingestor -> Bronze (reference).

Covers the spec's "Salesforce objects to Bronze": object configuration
(Account / Contact / Opportunity), incremental extraction via ``SystemModstamp``
or ``LastModifiedDate``, ``IsDeleted`` handling (soft deletes captured, not
dropped), raw object capture, audit columns, and an auth placeholder only.

Pure-Python and testable: the Salesforce API is abstracted behind a client with
``query(object, since, include_deleted) -> [records]``; tests inject an in-memory
org. In production this is the Salesforce managed connector / Bulk API writing a
Bronze Delta table, with the watermark in ``silver_control.ingestion_watermark``
— see ``docs/SALESFORCE_INGEST.md``.
"""
from __future__ import annotations

import json

from payments_platform.common.hashing import record_hash
from payments_platform.config import secrets
from payments_platform.bronze.jdbc_ingest import IngestionError  # reused error type

# audit columns this ingestor guarantees on every Bronze row
SALESFORCE_AUDIT_COLUMNS = (
    "source_system", "source_object", "load_type", "extract_time",
    "ingestion_timestamp", "run_id", "record_hash",
)

FULL = "full"
INCREMENTAL = "incremental"


# ---------------------------------------------------------------------------
# Reference Salesforce client
# ---------------------------------------------------------------------------
class InMemorySalesforce:
    """Test/reference org. ``objects`` = {object_name: [record dicts]}.

    ``query`` mimics SOQL incremental + ``queryAll`` semantics: filter on the
    modstamp field strictly greater than ``since``; include IsDeleted=true rows
    only when ``include_deleted`` (Salesforce's queryAll / Bulk 'queryAll')."""

    def __init__(self, objects, modstamp_field="SystemModstamp"):
        self.objects = objects
        self.modstamp_field = modstamp_field
        self.query_calls = 0

    def query(self, object_name, since=None, include_deleted=True):
        self.query_calls += 1
        rows = [dict(r) for r in self.objects.get(object_name, [])]
        if since is not None:
            rows = [r for r in rows
                    if r.get(self.modstamp_field) is not None
                    and r[self.modstamp_field] > since]
        if not include_deleted:
            rows = [r for r in rows if not r.get("IsDeleted", False)]
        return rows


class FailingSalesforce:
    """Always raises — used to test retry/failure handling."""

    def __init__(self, exc=RuntimeError("INVALID_SESSION_ID")):
        self.exc = exc
        self.query_calls = 0

    def query(self, object_name, since=None, include_deleted=True):
        self.query_calls += 1
        raise self.exc


# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------
def _auth_ref(config):
    """Resolve the OAuth client credentials as secret REFERENCES (never values)."""
    return {
        "client_id": secrets.get_secret("salesforce_client_id"),
        "client_secret": secrets.get_secret("salesforce_client_secret"),
    }


def _query_with_retry(client, object_name, since, include_deleted, max_retries):
    attempts = 0
    last = None
    while attempts <= max_retries:
        try:
            return client.query(object_name, since=since,
                                include_deleted=include_deleted)
        except Exception as exc:  # noqa: BLE001 — transient session/API error
            last = exc
            attempts += 1
    raise IngestionError("Salesforce query %s failed after %d attempts: %s"
                         % (object_name, max_retries + 1, last))


def _add_audit(row, config, ctx, load_type, modstamp_field):
    enriched = dict(row)
    enriched["source_system"] = config["source_system"]
    enriched["source_object"] = config["source_object"]
    enriched["load_type"] = load_type
    # extract_time = the record's modstamp (when Salesforce last changed it)
    enriched["extract_time"] = row.get(modstamp_field)
    enriched["ingestion_timestamp"] = ctx.ingestion_timestamp
    enriched["batch_id"] = ctx.batch_id
    enriched["run_id"] = ctx.run_id
    # IsDeleted -> operation_type so Silver can soft-delete in SCD
    enriched["operation_type"] = "DELETE" if row.get("IsDeleted") else "UPSERT"
    enriched["_raw_object"] = json.dumps(row, sort_keys=True, default=str)
    enriched["record_hash"] = record_hash(row, list(row.keys()))
    return enriched


def ingest_object(config, client, watermark_store, ctx, max_retries=2):
    """Ingest one configured Salesforce object into Bronze.

    Incremental uses the configured modstamp field (SystemModstamp or
    LastModifiedDate), strictly greater than the stored watermark; deleted rows
    are captured (IsDeleted -> operation_type DELETE), not dropped. The watermark
    advances forward only after a successful pull.
    """
    obj = config["source_object"]
    modstamp_field = config.get("watermark_column", "SystemModstamp")
    include_deleted = config.get("include_deleted", True)

    last_wm = watermark_store.get(obj)
    if config.get("load_type") == INCREMENTAL and last_wm is not None:
        load_type, since = INCREMENTAL, last_wm
    else:
        load_type, since = FULL, None

    _auth_ref(config)  # resolve auth references (never logged as values)
    rows = _query_with_retry(client, obj, since, include_deleted, max_retries)
    bronze = [_add_audit(r, config, ctx, load_type, modstamp_field) for r in rows]

    deleted = sum(1 for r in rows if r.get("IsDeleted"))

    # advance watermark forward only, from the modstamp field
    new_wm = last_wm
    field_vals = [r[modstamp_field] for r in rows if r.get(modstamp_field) is not None]
    if field_vals:
        batch_max = max(field_vals)
        if new_wm is None or batch_max > new_wm:
            new_wm = batch_max
        watermark_store.set(obj, new_wm)

    return {
        "source": obj,
        "source_object": obj,
        "status": "SUCCESS",
        "mode": load_type,
        "records_read": len(rows),
        "records_written": len(bronze),
        "deleted_records": deleted,
        "corrupt_records": 0,
        "watermark": watermark_store.get(obj),
        "bronze": bronze,
    }


def run_ingestion(configs, client, watermark_store, ctx, max_retries=2):
    """For-Each over configured objects. Disabled objects are skipped; a per-object
    failure is recorded and the run continues with the rest."""
    results = []
    for config in configs:
        if not config.get("enabled", True):
            results.append({"source": config["source_object"], "status": "SKIPPED",
                            "records_written": 0})
            continue
        try:
            results.append(ingest_object(config, client, watermark_store, ctx,
                                         max_retries))
        except IngestionError as exc:
            results.append({"source": config["source_object"], "status": "FAILED",
                            "records_written": 0, "error": str(exc)})
    return results
