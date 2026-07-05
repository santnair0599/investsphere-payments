"""
Bronze — Salesforce objects (real Databricks execution).

Pulls configured Salesforce objects (Account / Contact / Opportunity …) into Bronze
Delta via the REST/SOQL API, driven by ``seeds/salesforce/object_config.json``.

Auth (once per org, from the Key Vault-backed secret scope — never hardcoded):
  * **configured token flow** if ``salesforce-access-token`` (+ ``salesforce-instance-url``)
    are provisioned, else
  * **OAuth 2.0 username-password flow** (``salesforce-client-id`` / ``-client-secret`` /
    ``-username`` / ``-password``; store password+security-token in the password secret).

Per object:
  * incremental via the configured modstamp (``SystemModstamp`` / ``LastModifiedDate``)
    with the watermark **pushed down** into the SOQL ``WHERE`` (a Salesforce datetime
    literal, unquoted); first run is a full load;
  * ``queryAll`` so soft-deleted rows come through (``IsDeleted`` -> ``operation_type``);
  * pagination via ``nextRecordsUrl`` for large result sets;
  * records written to Bronze Delta with the raw object + audit columns; the watermark
    is advanced (forward only, in ``silver_control.ingestion_watermark``) **only after a
    successful write**;
  * **per-object try/except** so one failing object doesn't break the rest.

Reuses ``source_config`` + ``salesforce_ingest`` constants + ``config.secrets`` + the JDBC
watermark helpers + ``rest_ingest.RETRYABLE_STATUS``. ``requests``/``pyspark`` imported
inside ``run()`` (imports clean without Spark).
"""
from __future__ import annotations

import json
import os

from payments_platform.bronze import salesforce_ingest, source_config
from payments_platform.bronze.rest_ingest import RETRYABLE_STATUS
from payments_platform.bronze.jdbc_ingest import IngestionError
from payments_platform.config import secrets
from payments_platform.databricks.bronze_jdbc import (
    WATERMARK_TABLE, _read_watermark, _upsert_watermark)

API_VERSION = "59.0"
_TIMEOUT, _MAX_RETRIES, _MAX_PAGES = 30, 2, 1000
_COMPOUND_TYPES = ("address", "location")           # not selectable in SOQL

_DEFAULT_CONFIG = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..",
                 "seeds", "salesforce", "object_config.json"))


def _try_secret(dbutils, scope, key):
    try:
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:  # noqa: BLE001 — key not provisioned
        return None


def _request(requests, time, method, url, headers=None, params=None, data=None):
    """HTTP with retry/backoff on 429/5xx + network errors (honours Retry-After)."""
    backoff, last = 1.0, None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, headers=headers, params=params,
                                    data=data, timeout=_TIMEOUT)
        except Exception as exc:  # noqa: BLE001 — transient network error
            last = exc
            if attempt < _MAX_RETRIES:
                time.sleep(backoff); backoff *= 2; continue
            raise IngestionError("Salesforce %s %s failed (network): %s" % (method, url, exc))
        code = resp.status_code
        if 200 <= code < 300:
            return resp
        if code in RETRYABLE_STATUS:
            last = "HTTP %d" % code
            if attempt < _MAX_RETRIES:
                wait = backoff
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        pass
                time.sleep(wait); backoff *= 2; continue
            raise IngestionError("Salesforce %s failed after retries: HTTP %d" % (url, code))
        raise IngestionError("Salesforce %s failed: HTTP %d %s" % (url, code, resp.text[:200]))
    raise IngestionError("Salesforce %s failed: %s" % (url, last))


def _authenticate(requests, time, dbutils, scope):
    """Return (instance_url, access_token). Token flow if provisioned, else the
    OAuth 2.0 username-password flow."""
    token = _try_secret(dbutils, scope, "salesforce-access-token")
    if token:
        instance = dbutils.secrets.get(scope=scope, key="salesforce-instance-url")
        return instance.rstrip("/"), token
    login_url = (_try_secret(dbutils, scope, "salesforce-login-url")
                 or "https://login.salesforce.com").rstrip("/")
    data = {
        "grant_type": "password",
        "client_id": dbutils.secrets.get(scope=scope, key=secrets.SECRET_KEYS["salesforce_client_id"]),
        "client_secret": dbutils.secrets.get(scope=scope, key=secrets.SECRET_KEYS["salesforce_client_secret"]),
        "username": dbutils.secrets.get(scope=scope, key="salesforce-username"),
        "password": dbutils.secrets.get(scope=scope, key="salesforce-password"),
    }
    body = _request(requests, time, "POST", login_url + "/services/oauth2/token", data=data).json()
    return body["instance_url"].rstrip("/"), body["access_token"]


def _fields(requests, time, instance, headers, obj, cfg):
    """SOQL field list — configured ``fields`` or every selectable field from describe."""
    if cfg.get("fields"):
        fields = list(cfg["fields"])
    else:
        url = "%s/services/data/v%s/sobjects/%s/describe" % (instance, API_VERSION, obj)
        body = _request(requests, time, "GET", url, headers=headers).json()
        fields = [f["name"] for f in body.get("fields", [])
                  if f.get("type") not in _COMPOUND_TYPES]
    for required in (cfg.get("primary_key", "Id"),
                     cfg.get("watermark_column", "SystemModstamp"), "IsDeleted"):
        if required not in fields:
            fields.append(required)
    return fields


def _soql(obj, fields, modstamp, since):
    q = "SELECT %s FROM %s" % (",".join(fields), obj)
    if since is not None:
        q += " WHERE %s > %s" % (modstamp, since)   # SF datetime literal (unquoted) — pushed down
    return q


def _query_all(requests, time, instance, headers, soql, include_deleted):
    """Run the SOQL and follow ``nextRecordsUrl`` until done."""
    endpoint = "queryAll" if include_deleted else "query"
    url = "%s/services/data/v%s/%s" % (instance, API_VERSION, endpoint)
    body = _request(requests, time, "GET", url, headers=headers, params={"q": soql}).json()
    records = list(body.get("records", []))
    pages = 1
    while not body.get("done", True) and body.get("nextRecordsUrl") and pages < _MAX_PAGES:
        body = _request(requests, time, "GET", instance + body["nextRecordsUrl"], headers=headers).json()
        records.extend(body.get("records", []))
        pages += 1
    return records, pages


def _ingest_object(spark, catalog, cfg, run_id, requests, time, instance, headers):
    from pyspark.sql import functions as F

    obj = cfg["source_object"]
    target = "%s.%s" % (catalog, cfg["target_bronze_table"])
    wm_fqn = "%s.%s" % (catalog, WATERMARK_TABLE)
    modstamp = cfg.get("watermark_column", "SystemModstamp")
    include_deleted = cfg.get("include_deleted", True)

    last_wm = _read_watermark(spark, wm_fqn, obj)
    mode, since = salesforce_ingest.FULL, None
    if cfg.get("load_type") == salesforce_ingest.INCREMENTAL and last_wm is not None:
        mode, since = salesforce_ingest.INCREMENTAL, last_wm

    fields = _fields(requests, time, instance, headers, obj, cfg)
    records, pages = _query_all(requests, time, instance, headers,
                                _soql(obj, fields, modstamp, since), include_deleted)

    if not records:
        print("bronze_salesforce: %s -> 0 records [%s]" % (obj, mode))
        return {"source": obj, "mode": mode, "records_written": 0,
                "deleted_records": 0, "watermark": last_wm}

    # drop Salesforce 'attributes' metadata; keep everything else
    cleaned = [{k: v for k, v in r.items() if k != "attributes"} for r in records]
    df = spark.read.json(spark.sparkContext.parallelize(
        [json.dumps(r, default=str) for r in cleaned]))
    business_cols = df.columns
    raw_col = F.to_json(F.struct(*[F.col(c) for c in business_cols]))
    hash_col = F.sha2(F.concat_ws("|", *[
        F.coalesce(F.col(c).cast("string"), F.lit("")) for c in sorted(business_cols)]), 256)
    is_deleted = F.col("IsDeleted") if "IsDeleted" in business_cols else F.lit(False)
    src_extract = (F.col(modstamp).cast("timestamp") if modstamp in business_cols
                   else F.current_timestamp())

    bronze = (df
              .withColumn("_raw_object", raw_col)
              .withColumn("record_hash", hash_col)
              .withColumn("source_system", F.lit(cfg["source_system"]))
              .withColumn("source_object", F.lit(obj))
              .withColumn("load_type", F.lit(mode))
              .withColumn("operation_type",
                          F.when(is_deleted == True, F.lit("DELETE")).otherwise(F.lit("UPSERT")))  # noqa: E712
              .withColumn("run_id", F.lit(run_id))
              .withColumn("batch_id", F.lit(run_id))
              .withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("source_extract_timestamp", src_extract))

    (bronze.write.format("delta").mode("append")
        .option("mergeSchema", "true").saveAsTable(target))         # <-- must succeed first

    # advance watermark ONLY after the write, from the written rows (forward only)
    landed = spark.table(target).where(F.col("run_id") == run_id)
    written = landed.count()
    deleted = landed.where(F.col("operation_type") == "DELETE").count()
    new_wm = last_wm
    if modstamp in landed.columns and written > 0:
        batch_max = landed.agg(F.max(F.col(modstamp)).cast("string")).collect()[0][0]
        if batch_max is not None and (last_wm is None or batch_max > last_wm):
            new_wm = batch_max
            _upsert_watermark(spark, wm_fqn,
                              (obj, modstamp, new_wm, cfg.get("load_type", "full"), run_id))

    print("bronze_salesforce: %s -> %s [%s] pages=%d rows=%d deleted=%d wm=%s"
          % (obj, target, mode, pages, written, deleted, new_wm))
    return {"source": obj, "mode": mode, "pages": pages, "records_written": written,
            "deleted_records": deleted, "watermark": new_wm}


def run(catalog, run_id, config_path=None):
    """Authenticate once, then For-Each over the enabled Salesforce objects with
    per-object isolation. Returns the per-object summary."""
    import time
    import requests
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    scope = secrets.DEFAULT_SCOPE

    configs = source_config.load_configs(
        config_path or _DEFAULT_CONFIG, source_config.SALESFORCE_REQUIRED_KEYS)
    instance, token = _authenticate(requests, time, dbutils, scope)      # once per org
    headers = {"Authorization": "Bearer %s" % token}

    results = []
    for cfg in source_config.enabled_configs(configs):
        try:
            results.append(_ingest_object(spark, catalog, cfg, run_id, requests, time, instance, headers))
        except Exception as exc:  # noqa: BLE001 — isolate one object; keep going
            print("bronze_salesforce: FAILED %s: %s" % (cfg.get("source_object"), exc))
            results.append({"source": cfg.get("source_object"), "mode": "FAILED",
                            "records_written": 0, "error": str(exc)})
    print("bronze_salesforce: done —", len(results), "objects")
    return results
