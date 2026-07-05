"""
Least-privilege grant model, derived from :mod:`policy`.

A grant is a dict: {principal, privilege, securable_type, securable}. The builder
encodes the access matrix from the design:

  spn_investsphere_etl  read+write all base tables (the only writer)
  data_engineers        control + non-PII Gold + marts + engineer masked view;
                        NO SELECT on any PII base table or quarantine raw payload
  analysts              Gold marts + the masked-for-analytics view ONLY
  pii_approved_users    SELECT on PII base tables (mask functions reveal real)
  data_stewards         quarantine + control (review failed records)
"""
from __future__ import annotations

from payments_platform.governance import policy as P

USE_CATALOG = "USE CATALOG"
USE_SCHEMA = "USE SCHEMA"
SELECT = "SELECT"
MODIFY = "MODIFY"


def _g(principal, privilege, securable_type, securable):
    return {"principal": principal, "privilege": privilege,
            "securable_type": securable_type, "securable": securable}


def build_grants():
    """Return the full least-privilege grant list for the model."""
    grants = []

    # everyone with any access needs USE CATALOG
    for group in P.GROUPS:
        grants.append(_g(group, USE_CATALOG, "CATALOG", P.CATALOG))

    # ---- ETL service principal: read/write every base table ----
    etl = "spn_investsphere_etl"
    for schema in P.SCHEMAS:
        grants.append(_g(etl, USE_SCHEMA, "SCHEMA", P.CATALOG + "." + schema))
    for t in P.TABLES:
        grants.append(_g(etl, SELECT, "TABLE", P.table_fqn(t)))
        grants.append(_g(etl, MODIFY, "TABLE", P.table_fqn(t)))

    # ---- data engineers ----
    de = "data_engineers"
    for schema in ("silver_control", "gold", "gold_marts", "gold_masked"):
        grants.append(_g(de, USE_SCHEMA, "SCHEMA", P.CATALOG + "." + schema))
    for t in P.TABLES:
        # control + NON-PII gold tables + marts; never PII base / quarantine
        if t["tier"] in ("control", "mart") or (t["tier"] == "gold" and not t["pii"]):
            grants.append(_g(de, SELECT, "TABLE", P.table_fqn(t)))
    for v in P.VIEWS:
        if de in v["audience"]:
            grants.append(_g(de, SELECT, "VIEW", P.fqn(v["schema"], v["name"])))

    # ---- analysts: marts + masked views only ----
    an = "analysts"
    for schema in ("gold_marts", "gold_masked"):
        grants.append(_g(an, USE_SCHEMA, "SCHEMA", P.CATALOG + "." + schema))
    for t in P.TABLES:
        if t["tier"] == "mart":
            grants.append(_g(an, SELECT, "TABLE", P.table_fqn(t)))
    for v in P.VIEWS:
        if an in v["audience"]:
            grants.append(_g(an, SELECT, "VIEW", P.fqn(v["schema"], v["name"])))

    # ---- PII-approved users: SELECT on PII base (masks reveal real values) ----
    pii = "pii_approved_users"
    for schema in sorted({t["schema"] for t in P.pii_tables()}):
        grants.append(_g(pii, USE_SCHEMA, "SCHEMA", P.CATALOG + "." + schema))
    for t in P.pii_tables():
        grants.append(_g(pii, SELECT, "TABLE", P.table_fqn(t)))

    # ---- data stewards: quarantine + control ----
    st = "data_stewards"
    for schema in ("silver_quarantine", "silver_control"):
        grants.append(_g(st, USE_SCHEMA, "SCHEMA", P.CATALOG + "." + schema))
    for t in P.TABLES:
        if t["tier"] in ("quarantine", "control"):
            grants.append(_g(st, SELECT, "TABLE", P.table_fqn(t)))

    return grants
