"""
Generate Databricks-ready Unity Catalog SQL from :mod:`policy`.

Each function returns a SQL string; :func:`generate_all` returns an ordered
{filename: sql} map that ``generate_sql.py`` writes to ``governance/sql/``.

Notes on UC semantics:
  * Groups are account-level (SCIM/Terraform) — there is no CREATE GROUP DDL, so
    we emit a comment header listing the groups the grants assume.
  * Column masks / row filters are SQL UDFs in a ``governance`` schema, attached
    with ALTER TABLE ... SET MASK / SET ROW FILTER.
"""
from __future__ import annotations

from payments_platform.governance import policy as P
from payments_platform.governance import grants as G

GOV_SCHEMA = P.CATALOG + ".governance"


def _member_check(groups):
    return " OR ".join("is_account_group_member('%s')" % g for g in groups)


# ---- 00 catalog + schemas ---------------------------------------------------
def gen_catalog_schemas():
    lines = ["-- Catalog + schemas (idempotent).",
             "CREATE CATALOG IF NOT EXISTS %s;" % P.CATALOG,
             "CREATE SCHEMA IF NOT EXISTS %s;  -- mask/row-filter UDFs" % GOV_SCHEMA]
    for schema in P.SCHEMAS + ["gold_masked"]:
        lines.append("CREATE SCHEMA IF NOT EXISTS %s.%s;" % (P.CATALOG, schema))
    return "\n".join(lines) + "\n"


# ---- 01 PII tags ------------------------------------------------------------
def gen_tags():
    lines = ["-- PII classification tags on columns (Unity Catalog tags)."]
    for t in P.pii_tables():
        for col in t["pii"]:
            cls = P.PII_CLASS[col]
            lines.append(
                "ALTER TABLE %s ALTER COLUMN %s SET TAGS ('pii' = 'true', 'classification' = '%s');"
                % (P.table_fqn(t), col, cls))
    return "\n".join(lines) + "\n"


# ---- 02 mask functions ------------------------------------------------------
def gen_mask_functions():
    chk = _member_check(P.PII_UNMASK_GROUPS)
    bodies = {
        "mask_name":  "concat(left(val, 1), '***')",
        "mask_email": "regexp_replace(val, '(^.).*(@.*$)', '$1***$2')",
        "mask_phone": "concat('XXXXXX', right(val, 4))",
        "mask_id":    "sha2(val, 256)",
    }
    lines = ["-- Column-mask UDFs: privileged groups see real value, others masked."]
    for name, masked_expr in bodies.items():
        lines.append(
            "CREATE OR REPLACE FUNCTION %s.%s(val STRING)\n"
            "  RETURN CASE WHEN %s THEN val ELSE %s END;"
            % (GOV_SCHEMA, name, chk, masked_expr))
    return "\n\n".join(lines) + "\n"


# ---- 03 apply masks ---------------------------------------------------------
def gen_apply_masks():
    lines = ["-- Attach masks to PII columns on base tables."]
    for t in P.pii_tables():
        for col in t["pii"]:
            mask = P.CLASS_TO_MASK[P.PII_CLASS[col]]
            lines.append("ALTER TABLE %s ALTER COLUMN %s SET MASK %s.%s;"
                         % (P.table_fqn(t), col, GOV_SCHEMA, mask))
    return "\n".join(lines) + "\n"


# ---- 04 row filters ---------------------------------------------------------
def gen_row_filters():
    chk = _member_check(P.REGION_GLOBAL_GROUPS)
    lines = [
        "-- Region row filter: global groups see all; region teams (add as UC",
        "-- groups, e.g. region_uae) see only their region.",
        "CREATE OR REPLACE FUNCTION %s.region_filter(region STRING)" % GOV_SCHEMA,
        "  RETURN CASE",
        "    WHEN %s THEN true" % chk,
        "    WHEN is_account_group_member('region_' || lower(region)) THEN true",
        "    ELSE false",
        "  END;",
        "",
    ]
    for t in P.TABLES:
        if t["region_col"]:
            lines.append("ALTER TABLE %s SET ROW FILTER %s.region_filter ON (%s);"
                         % (P.table_fqn(t), GOV_SCHEMA, t["region_col"]))
    return "\n".join(lines) + "\n"


# ---- 05 grants --------------------------------------------------------------
def gen_grants():
    header = ("-- Groups are account-level (SCIM/Terraform); referenced here:\n"
              "--   " + ", ".join(P.GROUPS) + "\n")
    lines = [header]
    for grant in G.build_grants():
        lines.append("GRANT %s ON %s %s TO `%s`;" % (
            grant["privilege"], grant["securable_type"],
            grant["securable"], grant["principal"]))
    return "\n".join(lines) + "\n"


# ---- 06 masked views --------------------------------------------------------
def gen_masked_views():
    lines = ["-- Masked views: no raw PII; granted to the right audience."]
    for v in P.VIEWS:
        cols = list(v["passthrough"])
        for col in v.get("display_mask", []):
            mask = P.CLASS_TO_MASK[P.PII_CLASS[col]]
            cols.append("%s.%s(%s) AS %s" % (GOV_SCHEMA, mask, col, col))
        for col in v.get("hash_cols", []):
            cols.append("sha2(%s, 256) AS %s_hash" % (col, col))
        select = ",\n       ".join(cols)
        lines.append(
            "CREATE OR REPLACE VIEW %s.%s AS\nSELECT %s\nFROM %s.%s;"
            % (P.CATALOG, v["schema"] + "." + v["name"], select,
               P.CATALOG, v["base"]))
    return "\n\n".join(lines) + "\n"


def generate_all():
    """Ordered {filename: sql} for the whole governance deployment."""
    return {
        "00_catalog_schemas.sql": gen_catalog_schemas(),
        "01_pii_tags.sql": gen_tags(),
        "02_mask_functions.sql": gen_mask_functions(),
        "03_apply_masks.sql": gen_apply_masks(),
        "04_row_filters.sql": gen_row_filters(),
        "05_grants.sql": gen_grants(),
        "06_masked_views.sql": gen_masked_views(),
    }
