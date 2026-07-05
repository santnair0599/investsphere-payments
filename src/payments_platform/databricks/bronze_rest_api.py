"""
Bronze — metadata-driven REST API ingestion (real Databricks execution).

Pulls configured REST endpoints into Bronze Delta using ``requests`` on the driver,
driven by the same ``seeds/rest/api_config.json`` the reference ``bronze.rest_ingest``
uses. For each enabled endpoint:

  * **pagination** — page-number or cursor (config ``pagination.type``);
  * **incremental** — passes ``updated_since = <watermark>`` when configured and a
    watermark exists (first run pulls everything);
  * **retry/backoff** — retries ``RETRYABLE_STATUS`` (429/5xx) + network errors with
    exponential backoff, honouring ``Retry-After``;
  * **raw storage** — each record is written with its ``_raw_response`` JSON payload
    plus the inferred columns (semi-structured, schema-on-read).

Audit columns: ``source_system``, ``api_name``, ``endpoint``, ``run_id``,
``ingestion_timestamp``, ``source_extract_timestamp``, ``record_hash`` (+ ``_raw_response``,
``http_status``, ``batch_id``). The bearer token comes from the Key Vault-backed
secret scope (``dbutils.secrets``); the API base URL from config ``base_url`` or the
``rest-api-base-url`` secret. The watermark is persisted/advanced (forward-only,
**after a successful write**) in the shared ``silver_control.ingestion_watermark``
control table, keyed by ``api_name.endpoint``.

Reuses ``source_config`` + ``rest_ingest`` constants + ``config.secrets`` + the JDBC
watermark helpers. ``requests``/``pyspark`` imported inside ``run()`` (imports clean
without Spark).
"""
from __future__ import annotations

import json
import os

from payments_platform.bronze import source_config
from payments_platform.bronze.rest_ingest import CURSOR, PAGE, RETRYABLE_STATUS
from payments_platform.bronze.jdbc_ingest import IngestionError
from payments_platform.config import secrets
from payments_platform.databricks.bronze_jdbc import (
    WATERMARK_TABLE, _read_watermark, _upsert_watermark)

_DEFAULT_CONFIG = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "seeds", "rest", "api_config.json"))
_TIMEOUT = 30
_MAX_RETRIES = 2
_MAX_PAGES = 100


def _base_url(cfg, dbutils, scope):
    base = cfg.get("base_url") or dbutils.secrets.get(scope=scope, key="rest-api-base-url")
    return base.rstrip("/")


def _auth_headers(cfg, dbutils, scope):
    """Bearer token from the secret scope (never a hardcoded value)."""
    auth = cfg.get("auth") or {}
    if not auth:
        return {}
    logical = auth.get("secret_name", "rest_api_token")
    key = secrets.SECRET_KEYS.get(logical, logical)      # logical -> KV key
    token = dbutils.secrets.get(scope=scope, key=key)
    return {"Authorization": "%s %s" % (auth.get("scheme", "Bearer"), token)}


def _extract_records(body, cfg):
    """Pull the record list out of a JSON body (list, or under a configured/known key)."""
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        field = cfg.get("records_field")
        if field and isinstance(body.get(field), list):
            return body[field]
        for k in ("records", "data", "results", "items"):
            if isinstance(body.get(k), list):
                return body[k]
        return [body]                                    # single object
    return []


def _next_cursor(body, cfg):
    field = (cfg.get("pagination") or {}).get("cursor_response_field", "next_cursor")
    if isinstance(body, dict):
        return body.get(field) or (body.get("paging") or {}).get(field)
    return None


def _fetch_page(requests, time, session, url, params, headers, max_retries):
    """GET one page with retry/backoff on 429/5xx + network errors."""
    backoff, last = 1.0, None
    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 — transient network error
            last = exc
            if attempt < max_retries:
                time.sleep(backoff); backoff *= 2; continue
            raise IngestionError("GET %s failed (network): %s" % (url, exc))
        code = resp.status_code
        if 200 <= code < 300:
            return resp
        if code in RETRYABLE_STATUS:
            last = "HTTP %d" % code
            if attempt < max_retries:
                wait = backoff
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        pass
                time.sleep(wait); backoff *= 2; continue
            raise IngestionError("GET %s failed after retries: HTTP %d" % (url, code))
        raise IngestionError("GET %s failed: HTTP %d" % (url, code))   # non-retryable
    raise IngestionError("GET %s failed: %s" % (url, last))


def _paginate(requests, time, session, url, base_params, headers, cfg):
    """Walk pages (page-number or cursor) until exhausted or _MAX_PAGES."""
    pg = cfg.get("pagination") or {}
    mode = pg.get("type", PAGE)
    records, statuses, pages = [], [], 0
    if mode == CURSOR:
        cparam = pg.get("cursor_param", "cursor")
        cursor = None
        while pages < _MAX_PAGES:
            params = dict(base_params)
            if cursor is not None:
                params[cparam] = cursor
            resp = _fetch_page(requests, time, session, url, params, headers, _MAX_RETRIES)
            statuses.append(resp.status_code)
            body = resp.json()
            records.extend(_extract_records(body, cfg))
            pages += 1
            cursor = _next_cursor(body, cfg)
            if not cursor:
                break
    else:
        pparam, size = pg.get("page_param", "page"), pg.get("size")
        page = pg.get("start_page", 1)
        while pages < _MAX_PAGES:
            params = dict(base_params)
            params[pparam] = page
            resp = _fetch_page(requests, time, session, url, params, headers, _MAX_RETRIES)
            statuses.append(resp.status_code)
            recs = _extract_records(resp.json(), cfg)
            records.extend(recs)
            pages += 1
            if not recs or (size and len(recs) < size):    # empty/short page = last
                break
            page += 1
    return records, statuses, pages


def _ingest_one(spark, catalog, cfg, run_id, dbutils, requests, time):
    from pyspark.sql import functions as F

    api_name, endpoint = cfg["api_name"], cfg["endpoint"]
    target = "%s.%s" % (catalog, cfg["target_bronze_table"])
    wm_fqn = "%s.%s" % (catalog, WATERMARK_TABLE)
    wm = cfg.get("watermark") or {}
    wm_param, wm_field = wm.get("param", "updated_since"), wm.get("field")
    wm_key = "%s.%s" % (api_name, endpoint)
    scope = secrets.DEFAULT_SCOPE

    url = _base_url(cfg, dbutils, scope) + endpoint
    headers = _auth_headers(cfg, dbutils, scope)

    base_params = dict(cfg.get("request_params") or {})
    last_wm = _read_watermark(spark, wm_fqn, wm_key)
    mode = "full"
    if cfg.get("load_type") == "incremental" and last_wm is not None:
        base_params[wm_param] = last_wm
        mode = "incremental"

    session = requests.Session()
    records, statuses, pages = _paginate(requests, time, session, url, base_params, headers, cfg)
    last_status = statuses[-1] if statuses else None

    if not records:
        print("bronze_rest_api: %s -> 0 records [%s]" % (wm_key, mode))
        return {"source": api_name, "endpoint": endpoint, "mode": mode,
                "pages": pages, "records_written": 0, "watermark": last_wm}

    # semi-structured Bronze: raw payload + inferred columns
    df = spark.read.json(spark.sparkContext.parallelize(
        [json.dumps(r, sort_keys=True) for r in records]))
    business_cols = df.columns
    raw_col = F.to_json(F.struct(*[F.col(c) for c in business_cols]))
    hash_col = F.sha2(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in sorted(business_cols)]), 256)
    bronze = (df
              .withColumn("_raw_response", raw_col)
              .withColumn("record_hash", hash_col)
              .withColumn("source_system", F.lit(cfg["source_system"]))
              .withColumn("api_name", F.lit(api_name))
              .withColumn("endpoint", F.lit(endpoint))
              .withColumn("http_status", F.lit(last_status))
              .withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("source_extract_timestamp", F.current_timestamp())
              .withColumn("run_id", F.lit(run_id))
              .withColumn("batch_id", F.lit(run_id)))

    (bronze.write.format("delta").mode("append")
        .option("mergeSchema", "true").saveAsTable(target))       # <-- must succeed first

    # advance the watermark ONLY after the write, from the written rows (forward only)
    landed = spark.table(target).where(F.col("run_id") == run_id)
    written = landed.count()
    new_wm = last_wm
    if wm_field and wm_field in landed.columns and written > 0:
        batch_max = landed.agg(F.max(F.col(wm_field)).cast("string")).collect()[0][0]
        if batch_max is not None and (last_wm is None or batch_max > last_wm):
            new_wm = batch_max
            _upsert_watermark(spark, wm_fqn,
                              (wm_key, wm_field, new_wm, cfg.get("load_type", "full"), run_id))

    print("bronze_rest_api: %s -> %s [%s] pages=%d rows=%d wm=%s"
          % (wm_key, target, mode, pages, written, new_wm))
    return {"source": api_name, "endpoint": endpoint, "mode": mode, "pages": pages,
            "records_written": written, "watermark": new_wm}


def run(catalog, run_id, config_path=None):
    """For-Each over the enabled REST endpoints. A per-endpoint failure is logged
    and the run continues with the rest. Returns the per-endpoint summary."""
    import time
    import requests
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    configs = source_config.load_configs(
        config_path or _DEFAULT_CONFIG, source_config.REST_REQUIRED_KEYS)

    results = []
    for cfg in source_config.enabled_configs(configs):
        try:
            results.append(_ingest_one(spark, catalog, cfg, run_id, dbutils, requests, time))
        except Exception as exc:  # noqa: BLE001 — isolate one endpoint; keep going
            print("bronze_rest_api: FAILED %s: %s" % (cfg.get("api_name"), exc))
            results.append({"source": cfg.get("api_name"), "mode": "FAILED",
                            "records_written": 0, "error": str(exc)})
    print("bronze_rest_api: done —", len(results), "endpoints")
    return results
