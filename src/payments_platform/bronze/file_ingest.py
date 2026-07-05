"""
ADLS file feed -> Bronze (reference implementation).

Covers the spec's "Files from ADLS to Bronze": read records, capture file
name/path, capture corrupt records, add audit columns, keep raw + replayable.
No business transformation happens here (that's Silver).

Pure-Python so it runs locally over a CSV file or an in-memory list. On
Databricks the production counterpart is **Auto Loader**::

    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # _rescued_data
        .option("rescuedDataColumn", "_rescued_data")
        .load(landing_path)
        .select("*", "_metadata.file_path", "_metadata.file_name")
        .writeStream.option("checkpointLocation", ckpt).toTable(bronze_table))

Auto Loader tracks processed files (incremental), evolves schema, and routes
unparseable fields to ``_rescued_data`` — the same role as ``_corrupt_record``
here.
"""
from __future__ import annotations

import csv
import io
import os

from payments_platform.config.audit import add_audit_columns


def parse_csv(text, expected_columns):
    """Parse CSV text into (clean_rows, corrupt_rows).

    A row is *corrupt* when its field count doesn't match the header / expected
    columns — instead of dropping it, we keep the raw line so Bronze loses
    nothing and the bad record can be inspected/replayed.

    Returns:
        clean:   list of dicts keyed by header.
        corrupt: list of {"_raw": <line>, "_reason": <text>} dicts.
    """
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    clean, corrupt = [], []
    if not rows:
        return clean, corrupt

    header = rows[0]
    expected = list(expected_columns) if expected_columns else header

    for raw in rows[1:]:
        if len(raw) != len(header):
            corrupt.append({
                "_raw": ",".join(raw),
                "_reason": "field_count_mismatch expected=%d got=%d"
                           % (len(header), len(raw)),
            })
            continue
        record = dict(zip(header, raw))
        # guard against a header that doesn't carry the columns we require
        missing = [c for c in expected if c not in record]
        if missing:
            corrupt.append({
                "_raw": ",".join(raw),
                "_reason": "missing_columns " + ",".join(missing),
            })
            continue
        clean.append(record)
    return clean, corrupt


def ingest_records(clean_rows, corrupt_rows, ctx, business_columns,
                   source_file_name, source_file_path):
    """Turn parsed rows into Bronze rows with audit columns.

    Clean rows get ``operation_type='INSERT'`` and ``_corrupt_record=None``;
    corrupt rows are preserved with the raw text in ``_corrupt_record`` so they
    are queryable in Bronze and never silently lost.
    """
    bronze = []
    for row in clean_rows:
        bronze.append(add_audit_columns(
            row, ctx, business_columns,
            source_file_name=source_file_name,
            source_file_path=source_file_path,
            operation_type="INSERT",
        ))
    for bad in corrupt_rows:
        # corrupt rows carry no usable business payload -> empty hash input
        bronze.append(add_audit_columns(
            {}, ctx, business_columns,
            source_file_name=source_file_name,
            source_file_path=source_file_path,
            operation_type="INSERT",
            corrupt_record=bad["_raw"] + " :: " + bad["_reason"],
        ))
    return bronze


def ingest_file(path, ctx, business_columns, expected_columns=None):
    """Convenience: read a CSV file from disk and return Bronze rows."""
    with open(path, "r", encoding="utf-8", newline="") as handle:
        text = handle.read()
    clean, corrupt = parse_csv(text, expected_columns or business_columns)
    return ingest_records(
        clean, corrupt, ctx, business_columns,
        source_file_name=os.path.basename(path),
        source_file_path=path,
    )
