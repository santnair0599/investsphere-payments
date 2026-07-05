# SFTP / vendor-file ingestor

`bronze/sftp_ingest.py` copies vendor files delivered over SFTP into Bronze.
Pure-Python and testable: the server is abstracted behind a client
(`list_files()` / `get(name)`), so a caller can inject an in-memory drop (the smoke test does); in production
files land in an ADLS landing zone and Auto Loader (or a copy job) writes Bronze,
with processed files tracked in `silver_control.processed_files`.

## Config (source-config style)

Loaded with `source_config.load_configs(path, SFTP_REQUIRED_KEYS)`:

```json
{
  "source_system": "card_scheme_vendor",
  "file_pattern": "settlement_(?P<date>\\d{4}-\\d{2}-\\d{2})\\.csv",
  "business_columns": ["settlement_id", "merchant_id", "amount", "currency",
                       "settlement_date"],
  "target_bronze_table": "bronze.sftp_settlements",
  "source_dir": "sftp://card-vendor/incoming", "enabled": true
}
```

The `file_pattern` is a regex with a named `(?P<date>…)` group used for both
pattern validation and file-date extraction.

## Behaviour

| Requirement | How |
|---|---|
| File-pattern validation | `match_pattern` (regex `fullmatch`); a non-matching name → `BAD_PATTERN`, no Bronze rows |
| Expected file-date validation | `expected_filename_present` finds the file whose `date` group == the run's `expected_date`; absent → `MISSING_FILE` (late/missing) |
| Duplicate detection | `ProcessedFileRegistry` keyed by SHA-256 checksum; a re-seen file → `DUPLICATE`, no rows |
| Checksum / file hash | `file_checksum` = SHA-256 of the raw bytes |
| Corrupt/bad file | content that parses to zero clean rows (but isn't empty) → `CORRUPT` with a corrupt count |
| Raw-to-Bronze copy | clean rows + preserved corrupt rows (`_corrupt_record`) get audit columns |

`ingest_directory` returns an aggregate status: `SUCCESS` (file OK or duplicate),
`MISSING_FILE`, or `FAILED` (corrupt/bad). `MISSING_FILE` is the condition the
Bronze gate can require for a mandatory vendor feed.

## Audit columns

`source_system, source_file_name, source_file_path, file_date,
ingestion_timestamp, run_id, record_hash` (`SFTP_AUDIT_COLUMNS`), plus `batch_id`,
`operation_type`, `_corrupt_record`.

## Production mapping

`InMemorySftp` ↔ the SFTP/landing zone; `ProcessedFileRegistry` ↔
`silver_control.processed_files`; the CSV parse reuses `bronze/file_ingest.parse_csv`
(same corrupt-capture contract as the ADLS file feed, whose prod counterpart is
Auto Loader with `_rescued_data`). Run via the `bronze_sftp` DAG task, which
passes `run_date` as the expected file date and writes a `table_load_status` row.

## Behavior handled

Valid file, missing/late file, duplicate detection, bad pattern, corrupt file,
checksum stability, and the seed-config loader.
