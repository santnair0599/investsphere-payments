"""
Deterministic record hashing.

A ``record_hash`` lets us detect *real* changes cheaply: in CDC/SCD2 we only
expire-and-insert a row when the hash of the tracked columns actually changed,
so unchanged updates don't rewrite history (a key MERGE optimisation).

The hash must be stable across runs and machines, so we:
  * sort the columns by name,
  * normalise None -> "" and everything else via str(),
  * join with a separator that won't appear in normal data.

On Databricks the same value is produced with
``sha2(concat_ws('|', col1, col2, ...), 256)`` over the same ordered columns.
"""
from __future__ import annotations

import hashlib

_SEP = "|"


def record_hash(row, columns):
    """SHA-256 hex digest of the given columns of ``row`` (a dict).

    Args:
        row:     a dict-like record.
        columns: ordered iterable of column names to include in the hash.
    """
    parts = []
    for col in sorted(columns):
        value = row.get(col)
        parts.append("" if value is None else str(value))
    joined = _SEP.join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
