"""
Security validation — policy-as-code checks.

Each function returns a list of violation strings (empty = pass). The test-suite
asserts the real model is clean AND that a deliberately broken model is caught,
so the checks are proven to actually fail when governance is wrong.

Maps to the required failing conditions:
  * data_engineers get direct SELECT on PII base tables
  * PII columns do not have masks
  * quarantine raw payload is exposed
  * analyst/consumer views expose raw PII
"""
from __future__ import annotations

from payments_platform.governance import policy as P
from payments_platform.governance import grants as G


def no_engineer_select_on_pii_base(tables, grant_list):
    """data_engineers must have NO SELECT on any PII base table."""
    pii_fqns = {P.table_fqn(t) for t in tables if t["pii"]}
    violations = []
    for grant in grant_list:
        if (grant["principal"] == "data_engineers"
                and grant["privilege"] == G.SELECT
                and grant["securable"] in pii_fqns):
            violations.append(
                "data_engineers has SELECT on PII base table " + grant["securable"])
    return violations


def all_pii_columns_have_masks(tables):
    """Every PII column on every base PII table must map to a mask function."""
    violations = []
    for t in tables:
        for col in t["pii"]:
            cls = P.PII_CLASS.get(col)
            if cls is None:
                violations.append("%s.%s: column has no PII classification"
                                  % (t["name"], col))
                continue
            mask = P.CLASS_TO_MASK.get(cls)
            if mask is None or mask not in P.MASKS:
                violations.append("%s.%s: class '%s' has no mask function"
                                  % (t["name"], col, cls))
    return violations


def quarantine_not_exposed(tables, grant_list):
    """Only QUARANTINE_READ_GROUPS may SELECT raw-payload tables."""
    raw_fqns = {P.table_fqn(t) for t in tables if t["raw_payload"]}
    allowed = set(P.QUARANTINE_READ_GROUPS)
    violations = []
    for grant in grant_list:
        if (grant["privilege"] == G.SELECT
                and grant["securable"] in raw_fqns
                and grant["principal"] not in allowed):
            violations.append("%s may SELECT quarantine raw payload %s"
                              % (grant["principal"], grant["securable"]))
    return violations


def views_expose_no_raw_pii(views):
    """No masked view may expose a raw PII column."""
    violations = []
    for v in views:
        for col in v.get("exposed_pii", []):
            violations.append("%s exposes raw PII column %s"
                              % (v["name"], col))
        # a display_mask/hash list that overlaps with drop is fine; the contract
        # is: a PII column must be either dropped, display-masked, or hashed —
        # never selected raw.
        handled = set(v.get("display_mask", [])) | set(v.get("hash_cols", [])) \
            | set(v.get("drop_cols", []))
        # base PII columns of the view's source table
        base_schema, base_name = v["base"].split(".")
        base = next((t for t in P.TABLES
                     if t["schema"] == base_schema and t["name"] == base_name), None)
        if base:
            for col in base["pii"]:
                if col not in handled:
                    violations.append(
                        "%s passes through raw PII column %s (not masked/hashed/dropped)"
                        % (v["name"], col))
    return violations


def analysts_only_masked(tables, views, grant_list):
    """Analysts must not SELECT any base table (only marts + masked views)."""
    base_fqns = {P.table_fqn(t) for t in tables if t["tier"] != "mart"}
    violations = []
    for grant in grant_list:
        if (grant["principal"] == "analysts"
                and grant["privilege"] == G.SELECT
                and grant["securable_type"] == "TABLE"
                and grant["securable"] in base_fqns):
            violations.append("analysts has SELECT on base table " + grant["securable"])
    return violations


def validate_all(tables=None, views=None, grant_list=None):
    """Run every check; return a flat list of violations (empty = all pass)."""
    tables = P.TABLES if tables is None else tables
    views = P.VIEWS if views is None else views
    grant_list = G.build_grants() if grant_list is None else grant_list
    out = []
    out += no_engineer_select_on_pii_base(tables, grant_list)
    out += all_pii_columns_have_masks(tables)
    out += quarantine_not_exposed(tables, grant_list)
    out += views_expose_no_raw_pii(views)
    out += analysts_only_masked(tables, views, grant_list)
    return out
