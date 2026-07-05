# REST API ingestor

`bronze/rest_ingest.py` lands a paginated REST API source into Bronze. Pure-Python
and testable: the HTTP layer is abstracted behind a client (`fetch(endpoint,
params) -> RestResponse`), so a caller can inject an in-memory API (the smoke test does); in production it is a
paged `requests`/connector loop writing a Bronze Delta table.

## Config (source-config style)

A JSON list (Delta `silver_control.source_config` in prod), loaded with
`source_config.load_configs(path, REST_REQUIRED_KEYS)`:

```json
{
  "source_system": "partner_api", "api_name": "fx_rates_api",
  "endpoint": "/v1/fx/rates", "target_bronze_table": "bronze.rest_fx_rates",
  "primary_key": "rate_id", "load_type": "incremental", "enabled": true,
  "request_params": {"base": "AED"},
  "auth": {"scheme": "Bearer", "secret_name": "rest_api_token"},
  "pagination": {"type": "page", "page_param": "page", "size": 2},
  "watermark": {"param": "updated_since", "field": "updated_at"}
}
```

## Behaviour

| Requirement | How |
|---|---|
| Endpoint configuration | `endpoint` + `request_params` |
| Authentication / token placeholder | `auth.secret_name` → `config/secrets.get_secret` returns a secret **reference** (`{{secrets/scope/key}}`), never a value |
| Pagination | `pagination.type` = `page` (page-number) or `cursor`; walks until a short/empty page or null cursor, capped by `max_pages` |
| Rate-limit / retry | retries statuses in `RETRYABLE_STATUS` (429/5xx) and exceptions up to `max_retries` |
| Response status capture | every page's `status_code` is recorded (`status_codes` + per-row `status_code`) |
| Raw JSON storage | each row carries `_raw_response` (the raw page JSON) for replay |
| Incremental extraction | `updated_since` = stored watermark (or a cursor); watermark advances forward-only from `watermark.field` |
| Failed-response handling | a non-retryable status (e.g. 403) or exhausted retries raises `IngestionError`; the For-Each records the endpoint `FAILED` and continues |

## Audit columns

`source_system, api_name, endpoint, request_params, status_code,
ingestion_timestamp, run_id, record_hash` (`REST_AUDIT_COLUMNS`), plus `batch_id`
and `_raw_response`. `request_params` is stored as a JSON string.

## Production mapping

The reference `InMemoryRestApi` ↔ a real paged client. The watermark
(`api_name.endpoint` key) ↔ `silver_control.ingestion_watermark`. `_fetch_with_retry`
↔ connector retry/backoff. Run via the `bronze_rest_api` DAG task
(`pipelines/dag_task.py`), which writes a `table_load_status` row — see
[ORCHESTRATION](ORCHESTRATION.md) and [MONITORING](MONITORING.md).

## Behavior handled

The ingestor covers: first load, incremental, page + cursor pagination,
rate-limit retry, retry-on-exception, non-retryable + exhausted failures, audit
columns + raw capture, and the For-Each runner (disabled skipped, failure
isolated).
