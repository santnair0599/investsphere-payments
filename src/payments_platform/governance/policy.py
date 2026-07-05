"""
The single declarative Unity Catalog governance model for InvestSphere Payments.

Everything else (SQL generation, security validation, tests) reads from here.
Edit governance by editing this model — never by hand-writing divergent SQL.
"""
from __future__ import annotations

CATALOG = "investsphere"

# ---------------------------------------------------------------------------
# Groups (account-level; provisioned via SCIM/Terraform, referenced by grants).
# ---------------------------------------------------------------------------
GROUPS = [
    "data_engineers",       # build pipelines; NO raw-PII read
    "analysts",             # masked Gold marts / masked views only
    "pii_approved_users",   # may read unmasked PII (mask functions reveal)
    "data_stewards",        # review quarantine + control (incl. raw payload)
    "spn_investsphere_etl",  # ETL service principal: read/write base tables
]

# Privileged groups that mask functions reveal real values to.
PII_UNMASK_GROUPS = ["pii_approved_users"]
# Groups allowed to see quarantine raw payload.
QUARANTINE_READ_GROUPS = ["data_stewards", "pii_approved_users", "spn_investsphere_etl"]
# Groups that bypass the region row filter (see all regions).
REGION_GLOBAL_GROUPS = ["pii_approved_users", "data_stewards", "spn_investsphere_etl"]

# ---------------------------------------------------------------------------
# PII classification: column name -> classification, and class -> mask function.
# ---------------------------------------------------------------------------
PII_CLASS = {
    "customer_name": "name",
    "email": "email",
    "phone_number": "phone",
    "national_id": "national_id",
    "passport_number": "passport",
    "emirates_id": "national_id",
}

CLASS_TO_MASK = {
    "name": "mask_name",
    "email": "mask_email",
    "phone": "mask_phone",
    "national_id": "mask_id",
    "passport": "mask_id",
}

# Mask functions and which groups they reveal real values to.
MASKS = {
    "mask_name":  {"unmask_groups": PII_UNMASK_GROUPS},
    "mask_email": {"unmask_groups": PII_UNMASK_GROUPS},
    "mask_phone": {"unmask_groups": PII_UNMASK_GROUPS},
    "mask_id":    {"unmask_groups": PII_UNMASK_GROUPS},
}

# ---------------------------------------------------------------------------
# Tables. `pii` lists the PII columns; `raw_payload` flags quarantine payload;
# `region_col` enables a row filter; `tier` drives the grant model.
# ---------------------------------------------------------------------------
_CUSTOMER_PII = ["customer_name", "email", "phone_number",
                 "national_id", "passport_number", "emirates_id"]

TABLES = [
    {"schema": "silver_clean", "name": "payment_clean", "pii": [],
     "raw_payload": False, "region_col": None, "tier": "silver"},

    {"schema": "silver_cdc", "name": "customer_scd2", "pii": _CUSTOMER_PII,
     "raw_payload": False, "region_col": "nationality", "tier": "silver_pii"},

    {"schema": "silver_quarantine", "name": "failed_records", "pii": [],
     "raw_payload": True, "region_col": None, "tier": "quarantine"},

    {"schema": "silver_control", "name": "table_load_status", "pii": [],
     "raw_payload": False, "region_col": None, "tier": "control"},
    {"schema": "silver_control", "name": "pipeline_run", "pii": [],
     "raw_payload": False, "region_col": None, "tier": "control"},

    {"schema": "gold", "name": "dim_customer", "pii": _CUSTOMER_PII,
     "raw_payload": False, "region_col": "nationality", "tier": "gold_pii"},
    {"schema": "gold", "name": "dim_customer_history", "pii": _CUSTOMER_PII,
     "raw_payload": False, "region_col": "nationality", "tier": "gold_pii"},
    {"schema": "gold", "name": "fact_payments", "pii": [],
     "raw_payload": False, "region_col": None, "tier": "gold"},

    {"schema": "gold_marts", "name": "daily_payment_summary", "pii": [],
     "raw_payload": False, "region_col": "customer_country", "tier": "mart"},
]

SCHEMAS = sorted({t["schema"] for t in TABLES})

# ---------------------------------------------------------------------------
# Masked views. `audience` = groups granted the view. `exposed_pii` MUST be empty
# (raw PII columns the view leaks — validated). `display_mask` columns are shown
# masked; `hash_cols` are SHA-256 hashed (safe join keys for engineers).
# ---------------------------------------------------------------------------
# non-PII columns of gold.dim_customer that views pass through unchanged
_CUSTOMER_PASSTHROUGH = ["customer_id", "nationality", "status", "source_system"]

VIEWS = [
    # analysts: generic display-masked customer view (no raw PII)
    {"schema": "gold_masked", "name": "v_customer_masked_for_analytics",
     "base": "gold.dim_customer", "audience": ["analysts"],
     "passthrough": _CUSTOMER_PASSTHROUGH,
     "display_mask": ["customer_name", "email", "phone_number"],
     "drop_cols": ["national_id", "passport_number", "emirates_id"],
     "hash_cols": [], "exposed_pii": []},

    # data engineers: hashed keys for debugging joins, no raw PII
    {"schema": "gold_masked", "name": "v_customer_engineer",
     "base": "gold.dim_customer", "audience": ["data_engineers"],
     "passthrough": _CUSTOMER_PASSTHROUGH,
     "display_mask": [],
     "drop_cols": ["customer_name", "national_id", "passport_number", "emirates_id"],
     "hash_cols": ["email", "phone_number"], "exposed_pii": []},
]


# ---- small accessors -------------------------------------------------------
def pii_tables():
    return [t for t in TABLES if t["pii"]]


def quarantine_tables():
    return [t for t in TABLES if t["raw_payload"]]


def fqn(schema, name):
    return CATALOG + "." + schema + "." + name


def table_fqn(t):
    return fqn(t["schema"], t["name"])
