"""
Debezium CDC feed -> Bronze (reference implementation).

Covers the spec's "CDC source to Bronze": store insert/update/delete events,
keep operation/sequence/source-timestamp/before/after images, and do NOT apply
SCD yet (Silver does that). Bronze just safely lands the event stream.

Input is a Debezium envelope (as delivered on a Kafka topic), e.g.::

    {"op": "u", "ts_ms": 1719700000000,
     "before": {"id": 101, "email": "old@x.com", ...},
     "after":  {"id": 101, "email": "new@x.com", ...},
     "source": {"lsn": 12345, "ts_ms": 1719699999000, "table": "customer"}}

On Databricks the production counterpart reads the Kafka topic with Structured
Streaming, keeps the raw value + Kafka metadata (topic/partition/offset), and
writes to a Bronze Delta table — Silver then applies AUTO CDC / MERGE.
"""
from __future__ import annotations

from payments_platform.config.audit import add_audit_columns

# Debezium op codes -> our canonical operation_type
_OP_MAP = {
    "c": "INSERT",   # create
    "r": "READ",     # snapshot read
    "u": "UPDATE",   # update
    "d": "DELETE",   # delete
}


def _sequence_number(event):
    """Monotonic ordering key for an event.

    Prefer the source LSN (true commit order); fall back to source ts_ms, then
    the envelope ts_ms. Silver uses this to order out-of-order events.
    """
    source = event.get("source") or {}
    if source.get("lsn") is not None:
        return int(source["lsn"])
    if source.get("ts_ms") is not None:
        return int(source["ts_ms"])
    return int(event.get("ts_ms") or 0)


def normalize_event(event, ctx, business_columns, topic=None):
    """Normalise one Debezium envelope into a Bronze CDC row.

    The business payload is the ``after`` image (or ``before`` for a delete,
    since a delete has no after). We keep both images plus op/sequence/source-ts.
    """
    op = _OP_MAP.get(event.get("op"), "UNKNOWN")
    after = event.get("after")
    before = event.get("before")

    # business payload: after-image normally; before-image for deletes
    payload = after if after is not None else (before or {})

    source = event.get("source") or {}
    source_ts_ms = source.get("ts_ms") or event.get("ts_ms")

    row = dict(payload)
    # CDC-specific columns Silver relies on
    row["operation_type"] = op
    row["sequence_number"] = _sequence_number(event)
    row["event_timestamp"] = source_ts_ms
    row["_before_image"] = before
    row["_after_image"] = after
    row["_kafka_topic"] = topic

    bronze = add_audit_columns(
        row, ctx, business_columns,
        operation_type=op,
        source_file_name=None,
        source_file_path=None,
    )
    return bronze


def normalize_events(events, ctx, business_columns, topic=None):
    """Normalise a list of Debezium envelopes into Bronze CDC rows."""
    return [normalize_event(e, ctx, business_columns, topic) for e in events]
