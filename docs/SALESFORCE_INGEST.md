# Salesforce ingestor

`bronze/salesforce_ingest.py` lands Salesforce objects into Bronze. Pure-Python
and testable: the API is abstracted behind a client (`query(object, since,
include_deleted) -> [records]`), so a caller can inject an in-memory org (the smoke test does); in production
this is the Salesforce managed connector / Bulk API writing a Bronze Delta table.

## Config (source-config style)

Loaded with `source_config.load_configs(path, SALESFORCE_REQUIRED_KEYS)`:

```json
{
  "source_system": "salesforce", "source_object": "Account",
  "target_bronze_table": "bronze.sfdc_account", "primary_key": "Id",
  "watermark_column": "SystemModstamp", "load_type": "incremental",
  "include_deleted": true, "enabled": true
}
```

Configure one entry per object (Account / Contact / Opportunity / …);
`watermark_column` is `SystemModstamp` or `LastModifiedDate`.

## Behaviour

| Requirement | How |
|---|---|
| Object configuration | `source_object` (Account/Contact/Opportunity/…) per config |
| Incremental extraction | `query(..., since=watermark)` filters the modstamp strictly `>` the stored watermark; advances forward-only after success |
| `IsDeleted` handling | `include_deleted=true` uses queryAll semantics; deleted rows are **captured** (not dropped) with `operation_type=DELETE` so Silver can soft-delete in SCD; `deleted_records` is counted |
| Raw object capture | each row carries `_raw_object` (the full record as JSON) |
| Authentication placeholder | `config/secrets.get_secret('salesforce_client_id' / '…_secret')` returns secret **references** only — no real credentials |
| Retry / failure | `_query_with_retry` retries up to `max_retries`; exhausted → `IngestionError`; the For-Each records the object `FAILED` and continues |

## Audit columns

`source_system, source_object, load_type, extract_time, ingestion_timestamp,
run_id, record_hash` (`SALESFORCE_AUDIT_COLUMNS`), plus `batch_id`,
`operation_type` (UPSERT/DELETE), `_raw_object`. `extract_time` is the record's
modstamp (when Salesforce last changed it).

## Production mapping

`InMemorySalesforce` ↔ the Bulk API `queryAll`; the watermark (keyed by object) ↔
`silver_control.ingestion_watermark`. `operation_type=DELETE` rows flow to the
Silver SCD engine's soft-delete path (see the CDC/SCD2 logic). Run via the
`bronze_salesforce` DAG task, which writes a `table_load_status` row.

## Behavior handled

First full load + audit, incremental, IsDeleted captured as DELETE, exclude-deleted
mode, `LastModifiedDate` watermark, and retry/failure (raise + For-Each isolation).
