"""
Metadata-driven REST API ingestor -> Bronze (reference).

Covers the spec's "REST API source to Bronze": endpoint configuration, a token
auth placeholder, pagination (page-number OR cursor), rate-limit/retry handling,
response-status capture, raw-JSON storage, incremental extraction via
``updated_since`` or a cursor, audit columns, and failed-response handling.

Pure-Python and testable: the HTTP layer is abstracted behind a client with
``fetch(endpoint, params) -> RestResponse``; tests inject an in-memory API. In
production the same flow is a paged ``requests``/connector loop writing a Bronze
Delta table, with the watermark in ``silver_control.ingestion_watermark`` — see
``docs/REST_INGEST.md``.
"""
from __future__ import annotations

import json

from payments_platform.common.hashing import record_hash
from payments_platform.config import secrets
from payments_platform.bronze.jdbc_ingest import IngestionError  # reused error type

# audit columns this ingestor guarantees on every Bronze row
REST_AUDIT_COLUMNS = (
    "source_system", "api_name", "endpoint", "request_params", "status_code",
    "ingestion_timestamp", "run_id", "record_hash",
)

# HTTP statuses worth retrying (rate-limit + transient server errors)
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

PAGE, CURSOR = "page", "cursor"


class RestResponse:
    """A single API response page."""

    def __init__(self, status_code, records, next_cursor=None, raw=None):
        self.status_code = status_code
        self.records = records or []
        self.next_cursor = next_cursor
        # the raw JSON text exactly as returned (stored in Bronze for replay)
        self.raw = raw if raw is not None else json.dumps(
            {"status": status_code, "records": records, "next_cursor": next_cursor})

    @property
    def ok(self):
        return 200 <= self.status_code < 300


# ---------------------------------------------------------------------------
# Reference API clients
# ---------------------------------------------------------------------------
class InMemoryRestApi:
    """Test/reference API. ``datasets`` = {endpoint: [record dicts]}.

    Supports page-number and cursor pagination and an ``updated_since`` filter on
    an incremental field, mimicking a real paginated, filterable endpoint.
    """

    def __init__(self, datasets, page_size=2, incremental_field="updated_at"):
        self.datasets = datasets
        self.page_size = page_size
        self.incremental_field = incremental_field
        self.fetch_calls = 0

    def fetch(self, endpoint, params):
        self.fetch_calls += 1
        rows = [dict(r) for r in self.datasets.get(endpoint, [])]
        since = params.get("updated_since")
        if since is not None:
            rows = [r for r in rows
                    if r.get(self.incremental_field) is not None
                    and r[self.incremental_field] > since]
        rows.sort(key=lambda r: r.get(self.incremental_field, ""))

        # cursor pagination if a cursor param is present, else page-number
        if "cursor" in params:
            start = int(params.get("cursor") or 0)
        else:
            page = int(params.get("page", 1))
            start = (page - 1) * self.page_size
        window = rows[start:start + self.page_size]
        next_start = start + self.page_size
        next_cursor = str(next_start) if next_start < len(rows) else None
        return RestResponse(200, window, next_cursor=next_cursor)


class FlakyRestApi:
    """Wraps an API, returning a retryable status (or raising) the first
    ``fail_times`` calls — used to test rate-limit/retry handling."""

    def __init__(self, inner, fail_times=1, status=429, raise_exc=False):
        self.inner = inner
        self.fail_times = fail_times
        self.status = status
        self.raise_exc = raise_exc
        self.fetch_calls = 0

    def fetch(self, endpoint, params):
        self.fetch_calls += 1
        if self.fetch_calls <= self.fail_times:
            if self.raise_exc:
                raise ConnectionError("connection reset")
            return RestResponse(self.status, [])
        return self.inner.fetch(endpoint, params)


class FailingRestApi:
    """Always returns a non-retryable failure status — used to test failed
    API-response handling (the ingestor raises after exhausting retries)."""

    def __init__(self, status=403):
        self.status = status
        self.fetch_calls = 0

    def fetch(self, endpoint, params):
        self.fetch_calls += 1
        return RestResponse(self.status, [])


# ---------------------------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------------------------
def _auth_header(config):
    """Resolve the auth token as a secret REFERENCE (never a real value)."""
    auth = config.get("auth") or {}
    if not auth:
        return None
    secret_name = auth.get("secret_name", "rest_api_token")
    return {auth.get("scheme", "Bearer"): secrets.get_secret(secret_name)}


def _fetch_with_retry(client, endpoint, params, max_retries):
    """Fetch one page, retrying retryable statuses / exceptions.

    Returns the successful RestResponse. Raises IngestionError when the response
    is a non-retryable failure or retries are exhausted.
    """
    attempts = 0
    last = None
    while attempts <= max_retries:
        try:
            resp = client.fetch(endpoint, params)
        except Exception as exc:  # noqa: BLE001 — transient network error
            last = exc
            attempts += 1
            continue
        if resp.ok:
            return resp
        if resp.status_code in RETRYABLE_STATUS:
            last = "HTTP %d" % resp.status_code
            attempts += 1
            continue
        # non-retryable failure (4xx other than 429): stop immediately
        raise IngestionError("API %s failed: HTTP %d" % (endpoint, resp.status_code))
    raise IngestionError("API %s failed after %d attempts: %s"
                         % (endpoint, max_retries + 1, last))


def _add_audit(row, config, ctx, status_code, request_params, raw):
    enriched = dict(row)
    enriched["source_system"] = config["source_system"]
    enriched["api_name"] = config["api_name"]
    enriched["endpoint"] = config["endpoint"]
    enriched["request_params"] = json.dumps(request_params, sort_keys=True)
    enriched["status_code"] = status_code
    enriched["ingestion_timestamp"] = ctx.ingestion_timestamp
    enriched["batch_id"] = ctx.batch_id
    enriched["run_id"] = ctx.run_id
    enriched["_raw_response"] = raw
    enriched["record_hash"] = record_hash(row, list(row.keys()))
    return enriched


def ingest_endpoint(config, client, watermark_store, ctx, max_retries=2,
                    max_pages=100):
    """Ingest one configured REST endpoint into Bronze.

    Pagination walks pages until the API reports no more (cursor None / short
    page) or ``max_pages`` is hit. Incremental runs pass ``updated_since`` = the
    stored watermark; the watermark advances (forward only) after a successful
    pull. The watermark is keyed by ``api_name.endpoint``.
    """
    wm = config.get("watermark") or {}
    wm_param = wm.get("param", "updated_since")
    wm_field = wm.get("field")
    pagination = config.get("pagination") or {}
    mode = pagination.get("type", PAGE)
    wm_key = "%s.%s" % (config["api_name"], config["endpoint"])

    base_params = dict(config.get("request_params") or {})
    last_wm = watermark_store.get(wm_key)
    load_mode = "full"
    if config.get("load_type") == "incremental" and last_wm is not None:
        base_params[wm_param] = last_wm
        load_mode = "incremental"

    # auth resolved but only its reference is carried (never logged as a value)
    _auth_header(config)

    bronze, status_codes = [], []
    pages = 0
    cursor, page = None, 1
    while pages < max_pages:
        params = dict(base_params)
        if mode == CURSOR:
            params["cursor"] = cursor or 0
        else:
            params["page"] = page
        resp = _fetch_with_retry(client, config["endpoint"], params, max_retries)
        status_codes.append(resp.status_code)
        pages += 1
        for rec in resp.records:
            bronze.append(_add_audit(rec, config, ctx, resp.status_code,
                                     params, resp.raw))
        if mode == CURSOR:
            if resp.next_cursor is None:
                break
            cursor = resp.next_cursor
        else:
            if not resp.records or resp.next_cursor is None:
                break
            page += 1

    # advance watermark (forward only) from the incremental field
    new_wm = last_wm
    if wm_field:
        field_vals = [r[wm_field] for r in bronze if r.get(wm_field) is not None]
        if field_vals:
            batch_max = max(field_vals)
            if new_wm is None or batch_max > new_wm:
                new_wm = batch_max
        if new_wm is not None:
            watermark_store.set(wm_key, new_wm)

    return {
        "source": config["api_name"],
        "api_name": config["api_name"],
        "endpoint": config["endpoint"],
        "status": "SUCCESS",
        "mode": load_mode,
        "pages": pages,
        "status_codes": status_codes,
        "records_read": len(bronze),
        "records_written": len(bronze),
        "corrupt_records": 0,
        "watermark": watermark_store.get(wm_key),
        "bronze": bronze,
    }


def run_ingestion(configs, client, watermark_store, ctx, max_retries=2):
    """For-Each over configured endpoints. Disabled endpoints are skipped; a
    per-endpoint failure is recorded and the run continues with the rest."""
    results = []
    for config in configs:
        if not config.get("enabled", True):
            results.append({"source": config["api_name"], "status": "SKIPPED",
                            "records_written": 0})
            continue
        try:
            results.append(ingest_endpoint(config, client, watermark_store, ctx,
                                           max_retries))
        except IngestionError as exc:
            results.append({"source": config["api_name"], "status": "FAILED",
                            "records_written": 0, "error": str(exc)})
    return results
