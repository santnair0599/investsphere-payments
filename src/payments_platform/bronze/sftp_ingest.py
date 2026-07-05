"""
SFTP / vendor-file ingestor -> Bronze (reference).

Covers the spec's "Vendor files over SFTP to Bronze": file-pattern validation,
expected-file-date validation (missing / late detection), duplicate-file
detection via checksum, file hashing, corrupt/bad-file handling, a raw-to-Bronze
copy simulation, and audit columns.

Pure-Python and testable: the SFTP server is abstracted behind a client with
``list_files()`` / ``get(name)``; tests inject an in-memory drop. In production
the same flow lands files in an ADLS landing zone and Auto Loader (or a copy job)
writes Bronze, with processed files tracked in ``silver_control.processed_files``
— see ``docs/SFTP_INGEST.md``.
"""
from __future__ import annotations

import hashlib
import re

from payments_platform.common.hashing import record_hash
from payments_platform.bronze import file_ingest

# audit columns this ingestor guarantees on every Bronze row
SFTP_AUDIT_COLUMNS = (
    "source_system", "source_file_name", "source_file_path", "file_date",
    "ingestion_timestamp", "run_id", "record_hash",
)

# per-file outcomes
OK = "SUCCESS"
DUPLICATE = "DUPLICATE"
BAD_PATTERN = "BAD_PATTERN"
CORRUPT = "CORRUPT"
MISSING = "MISSING_FILE"


# ---------------------------------------------------------------------------
# Reference SFTP client + processed-file registry
# ---------------------------------------------------------------------------
class InMemorySftp:
    """Test/reference SFTP drop. ``files`` = {filename: text content}."""

    def __init__(self, files, base_path="sftp://vendor/incoming"):
        self.files = dict(files)
        self.base_path = base_path
        self.get_calls = 0

    def list_files(self):
        return sorted(self.files)

    def get(self, name):
        self.get_calls += 1
        return self.files[name]

    def path(self, name):
        return self.base_path + "/" + name


class ProcessedFileRegistry:
    """Tracks already-ingested files by checksum (idempotency / dup detection).

    Reference for the Delta ``silver_control.processed_files`` table.
    """

    def __init__(self, seen=None):
        self._by_checksum = dict(seen or {})

    def seen(self, checksum):
        return checksum in self._by_checksum

    def register(self, name, checksum):
        self._by_checksum[checksum] = name

    def snapshot(self):
        return dict(self._by_checksum)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------
def file_checksum(content):
    """SHA-256 of the raw file bytes (the file hash)."""
    data = content.encode("utf-8") if isinstance(content, str) else content
    return hashlib.sha256(data).hexdigest()


def match_pattern(filename, pattern):
    """True if ``filename`` matches the configured regex ``pattern``."""
    return re.fullmatch(pattern, filename) is not None


def extract_file_date(filename, pattern):
    """Pull the ``date`` named group out of the pattern (None if absent)."""
    m = re.fullmatch(pattern, filename)
    if not m:
        return None
    try:
        return m.group("date")
    except IndexError:        # pattern has no (?P<date>...) group
        return None


def expected_filename_present(filenames, pattern, expected_date):
    """Return the file matching ``expected_date`` (or None -> missing/late)."""
    for name in sorted(filenames):
        if match_pattern(name, pattern) and extract_file_date(name, pattern) == expected_date:
            return name
    return None


# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------
def _add_audit(row, config, ctx, source_file_name, source_file_path, file_date,
               business_columns, corrupt_record=None):
    enriched = dict(row)
    enriched["source_system"] = config["source_system"]
    enriched["source_file_name"] = source_file_name
    enriched["source_file_path"] = source_file_path
    enriched["file_date"] = file_date
    enriched["ingestion_timestamp"] = ctx.ingestion_timestamp
    enriched["batch_id"] = ctx.batch_id
    enriched["run_id"] = ctx.run_id
    enriched["operation_type"] = "INSERT"
    enriched["_corrupt_record"] = corrupt_record
    enriched["record_hash"] = record_hash(row, business_columns)
    return enriched


def ingest_file(filename, content, config, ctx, registry, source_file_path):
    """Validate + copy one vendor file into Bronze rows.

    Order of checks: pattern -> duplicate (checksum) -> parse/corrupt. A rejected
    or duplicate file writes no Bronze rows but returns a status row so the run is
    auditable.
    """
    pattern = config["file_pattern"]
    business_columns = config["business_columns"]

    if not match_pattern(filename, pattern):
        return {"source_file_name": filename, "status": BAD_PATTERN,
                "records_written": 0, "corrupt_records": 0, "bronze": []}

    checksum = file_checksum(content)
    if registry.seen(checksum):
        return {"source_file_name": filename, "status": DUPLICATE,
                "checksum": checksum, "records_written": 0,
                "corrupt_records": 0, "bronze": []}

    file_date = extract_file_date(filename, pattern)
    clean, corrupt = file_ingest.parse_csv(content, business_columns)

    # a file that parses to zero clean rows but has content is treated as corrupt
    if not clean and content.strip():
        registry.register(filename, checksum)
        return {"source_file_name": filename, "status": CORRUPT,
                "checksum": checksum, "file_date": file_date,
                "records_written": 0, "corrupt_records": max(1, len(corrupt)),
                "bronze": []}

    bronze = [_add_audit(r, config, ctx, filename, source_file_path, file_date,
                         business_columns) for r in clean]
    bronze += [_add_audit({}, config, ctx, filename, source_file_path, file_date,
                          business_columns,
                          corrupt_record=bad["_raw"] + " :: " + bad["_reason"])
               for bad in corrupt]
    registry.register(filename, checksum)
    return {"source_file_name": filename, "status": OK, "checksum": checksum,
            "file_date": file_date, "records_written": len(clean),
            "corrupt_records": len(corrupt), "bronze": bronze}


def ingest_directory(config, client, ctx, registry, expected_date):
    """Ingest the vendor file for ``expected_date``.

    Returns an aggregate result with ``status`` SUCCESS / MISSING_FILE / FAILED
    and the per-file detail. MISSING_FILE means the expected dated file never
    arrived (late or absent) — a hard condition the Bronze gate can require.
    """
    pattern = config["file_pattern"]
    name = expected_filename_present(client.list_files(), pattern, expected_date)
    if name is None:
        return {"source": config["source_system"], "status": MISSING,
                "expected_date": expected_date, "records_written": 0,
                "corrupt_records": 0, "files": [], "bronze": []}

    res = ingest_file(name, client.get(name), config, ctx, registry,
                      client.path(name))
    status = "SUCCESS" if res["status"] in (OK, DUPLICATE) else "FAILED"
    return {
        "source": config["source_system"],
        "status": status,
        "expected_date": expected_date,
        "records_written": res["records_written"],
        "corrupt_records": res.get("corrupt_records", 0),
        "files": [res],
        "bronze": res.get("bronze", []),
    }
