"""
Silver deduplication.

Covers the spec's Silver step 5: Bronze may hold duplicate files, duplicate CDC
events, duplicate Kafka messages, API-retry duplicates. Silver deduplicates by a
business key, keeping the latest by an ordering column (sequence / timestamp).

On Databricks this is ``dropDuplicates`` (+ watermark for streaming) or a
window ``row_number() over (partition by key order by seq desc) = 1``.
"""
from __future__ import annotations


def dedup_latest(rows, key_columns, order_column):
    """Keep one row per key — the one with the greatest ``order_column``.

    Args:
        rows:         list of dicts.
        key_columns:  iterable of column names forming the business key.
        order_column: column whose max wins (e.g. sequence_number / event_ts).

    Ties (equal order value) keep the last occurrence seen, which is stable for
    replays that re-deliver identical events.
    """
    key_columns = list(key_columns)
    best = {}
    for row in rows:
        key = tuple(row.get(c) for c in key_columns)
        current = best.get(key)
        if current is None or _order_val(row, order_column) >= _order_val(current, order_column):
            best[key] = row
    return list(best.values())


def _order_val(row, order_column):
    value = row.get(order_column)
    # None sorts lowest so a real value always wins over a missing one
    return (value is not None, value)
