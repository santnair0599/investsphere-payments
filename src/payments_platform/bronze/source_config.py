"""
Load the metadata-driven source configuration.

Reference loader over a JSON file; in production this is the Delta control table
``silver_control.source_config`` read with ``spark.table(...)``. Same columns:
source_system, source_table, target_bronze_table, primary_key, watermark_column,
load_type, enabled.
"""
from __future__ import annotations

import json

REQUIRED_KEYS = ("source_system", "source_table", "target_bronze_table",
                 "primary_key", "load_type", "enabled")


def load_source_config(path):
    """Read and validate the source-config list from a JSON file."""
    with open(path, "r", encoding="utf-8") as handle:
        configs = json.load(handle)
    for cfg in configs:
        missing = [k for k in REQUIRED_KEYS if k not in cfg]
        if missing:
            raise ValueError("source_config %s missing keys: %s"
                             % (cfg.get("source_table"), missing))
        if cfg["load_type"] == "incremental" and not cfg.get("watermark_column"):
            raise ValueError("incremental table %s needs a watermark_column"
                             % cfg["source_table"])
    return configs


def enabled_configs(configs):
    return [c for c in configs if c.get("enabled", True)]


def load_configs(path, required_keys):
    """Generic metadata-config loader for the REST / SFTP / Salesforce ingestors.

    Same shape as the JDBC ``source_config`` table — a JSON list of dicts — but
    each source type declares its own ``required_keys`` (so e.g. REST needs
    ``api_name``/``endpoint`` while SFTP needs ``file_pattern``). Validates that
    every config carries those keys; raises ValueError otherwise.
    """
    with open(path, "r", encoding="utf-8") as handle:
        configs = json.load(handle)
    for cfg in configs:
        missing = [k for k in required_keys if k not in cfg]
        if missing:
            raise ValueError("source_config %s missing keys: %s"
                             % (cfg.get("target_bronze_table", cfg), missing))
    return configs


# required-key sets per source type (the "source config style" each ingestor reads)
REST_REQUIRED_KEYS = ("source_system", "api_name", "endpoint",
                      "target_bronze_table", "load_type", "enabled")
SFTP_REQUIRED_KEYS = ("source_system", "file_pattern", "business_columns",
                      "target_bronze_table", "enabled")
SALESFORCE_REQUIRED_KEYS = ("source_system", "source_object",
                            "target_bronze_table", "load_type", "enabled")
