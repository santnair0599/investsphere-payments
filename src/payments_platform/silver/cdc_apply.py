"""
CDC apply engine: SCD Type 1 and Type 2 (reference implementation).

Covers the spec's Silver steps 6-9: apply insert/update/delete from a CDC feed,
maintain current (SCD1) or full history (SCD2), handle deletes as soft-delete /
SCD2-expire, and tolerate out-of-order events via a sequence number.

This is the pure-Python equivalent of Databricks **AUTO CDC** (a.k.a. APPLY
CHANGES) with ``SEQUENCE BY sequence_number`` and ``STORED AS SCD TYPE 2``, or a
hand-written ``foreachBatch`` + Delta ``MERGE``. The logic — order by sequence,
detect real change by hash, expire-and-insert, soft-delete — is identical; only
the execution engine differs.

SCD2 columns produced: effective_from, effective_to, is_current, is_deleted,
record_hash, sequence_number.
"""
from __future__ import annotations

from payments_platform.common.hashing import record_hash

_MIN_SEQ = float("-inf")


def _seq(event):
    value = event.get("sequence_number")
    return _MIN_SEQ if value is None else value


def _version_row(event, key, tracked, effective_from, seq, is_deleted):
    """Build a new SCD2 version (the current row) from an event."""
    row = {key: event.get(key)}
    for col in tracked:
        row[col] = event.get(col)
    row["effective_from"] = effective_from
    row["effective_to"] = None
    row["is_current"] = True
    row["is_deleted"] = is_deleted
    row["record_hash"] = record_hash(event, tracked)
    row["sequence_number"] = seq
    row["source_system"] = event.get("source_system")
    return row


def apply_scd2(existing, events, key, tracked):
    """Apply CDC events to an SCD2 dimension and return the full new state.

    Args:
        existing: current SCD2 rows (history + current). May be empty.
        events:   conformed CDC rows with operation_type / sequence_number /
                  event_timestamp (e.g. from ``parse.standardize_customer``).
        key:      business key column (e.g. "customer_id").
        tracked:  columns whose change triggers a new version.

    Returns:
        The complete SCD2 table after applying the events.
    """
    result = [dict(r) for r in existing]

    # index of the current (is_current) row per key
    current_idx = {}
    for i, row in enumerate(result):
        if row.get("is_current"):
            current_idx[row.get(key)] = i

    # Creation sequence per key (earliest INSERT/READ in this batch). An
    # UPDATE/DELETE with a lower sequence than the row's creation is impossible
    # in real CDC -> it's a stale/out-of-order replay and must be dropped, even
    # though sorting would otherwise process it first.
    create_seq = {}
    for event in events:
        if event.get("operation_type") in ("INSERT", "READ"):
            k = event.get(key)
            s = _seq(event)
            if k not in create_seq or s < create_seq[k]:
                create_seq[k] = s

    for event in sorted(events, key=_seq):
        k = event.get(key)
        op = event.get("operation_type")
        seq = _seq(event)
        event_ts = event.get("event_timestamp")
        idx = current_idx.get(k)
        current = result[idx] if idx is not None else None

        # stale guard: an UPDATE/DELETE older than the key's creation is a
        # late/out-of-order replay -> drop it.
        if op in ("UPDATE", "DELETE") and k in create_seq and seq < create_seq[k]:
            continue

        # out-of-order / duplicate guard: ignore events not newer than the
        # event that produced the current version.
        if current is not None and seq <= current.get("sequence_number", _MIN_SEQ):
            continue

        if op == "DELETE":
            if current is None:
                continue  # delete for an unknown key -> nothing to expire
            # expire the live row and append a current "deleted" version
            result[idx]["is_current"] = False
            result[idx]["effective_to"] = event_ts
            deleted = {key: current.get(key)}
            for col in tracked:
                deleted[col] = current.get(col)
            deleted.update(
                effective_from=event_ts, effective_to=None, is_current=True,
                is_deleted=True, record_hash=current.get("record_hash"),
                sequence_number=seq, source_system=event.get("source_system"),
            )
            result.append(deleted)
            current_idx[k] = len(result) - 1
            continue

        # INSERT / UPDATE / READ
        new_hash = record_hash(event, tracked)
        if current is None:
            result.append(_version_row(event, key, tracked, event_ts, seq, False))
            current_idx[k] = len(result) - 1
        elif current.get("record_hash") == new_hash and not current.get("is_deleted"):
            continue  # no real change -> don't rewrite history
        else:
            result[idx]["is_current"] = False
            result[idx]["effective_to"] = event_ts
            result.append(_version_row(event, key, tracked, event_ts, seq, False))
            current_idx[k] = len(result) - 1

    return result


def apply_scd1(existing, events, key, tracked):
    """Apply CDC events keeping only the latest record per key (SCD1).

    Deletes are soft (``is_deleted=True``); out-of-order events are ignored via
    the sequence number.
    """
    current = {row.get(key): dict(row) for row in existing}
    last_seq = {k: v.get("sequence_number", _MIN_SEQ) for k, v in current.items()}

    for event in sorted(events, key=_seq):
        k = event.get(key)
        op = event.get("operation_type")
        seq = _seq(event)
        event_ts = event.get("event_timestamp")

        if k in last_seq and seq <= last_seq[k]:
            continue  # stale / duplicate

        if op == "DELETE":
            if k in current:
                current[k]["is_deleted"] = True
                current[k]["updated_at"] = event_ts
                last_seq[k] = seq
            continue

        row = {key: event.get(k) if False else event.get(key)}
        for col in tracked:
            row[col] = event.get(col)
        row.update(
            is_deleted=False, updated_at=event_ts, sequence_number=seq,
            record_hash=record_hash(event, tracked),
            source_system=event.get("source_system"),
        )
        current[k] = row
        last_seq[k] = seq

    return list(current.values())
