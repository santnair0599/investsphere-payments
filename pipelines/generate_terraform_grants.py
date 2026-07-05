"""
Generate Terraform inputs from the governance policy-as-code model, so the infra
grants can never drift from the security policy the tests enforce.

    python pipelines/generate_terraform_grants.py

Writes:
  infra/terraform/generated/schemas.auto.tfvars.json  -> {"schemas": [...]}
  infra/terraform/generated/grants.auto.tfvars.json   -> catalog + schema grants

Terraform owns catalog/schema-level grants (USE CATALOG / USE SCHEMA); table /
column-mask / row-filter / view grants are owned by the governance SQL
(governance/sql/, slice 2). See docs/TERRAFORM.md for the ownership split.
"""
from __future__ import annotations

import json
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys  # noqa: E402
sys.path.insert(0, os.path.join(_ROOT, "src"))

from payments_platform.governance import grants as G       # noqa: E402
from payments_platform.governance import policy as P        # noqa: E402

OUT_DIR = os.path.join(_ROOT, "infra", "terraform", "generated")

# Canonical infra schema set. NOTE: the design doc's "gold_mart" is provisioned as
# "gold_marts" (matching the dbt project + governance grants), and "gold_masked"
# is added because the masked views live there.
INFRA_SCHEMAS = [
    "bronze", "silver_clean", "silver_cdc", "silver_quarantine", "silver_control",
    "gold", "gold_marts", "gold_masked", "governance", "monitoring",
]

# Schemas that hold raw / PII data — data_engineers & analysts must NOT get a
# path to them at the schema level (defense in depth on top of column masks).
PII_OR_RAW_SCHEMAS = {"bronze", "silver_clean", "silver_cdc", "silver_quarantine"}

# Infra-only schemas not covered by the (table-derived) policy grant model, with
# the principals that need USE SCHEMA on them.
INFRA_SCHEMA_EXTRA_GRANTS = {
    "bronze": ["spn_investsphere_etl"],
    "gold_masked": ["spn_investsphere_etl"],
    "governance": ["spn_investsphere_etl"],
    "monitoring": ["spn_investsphere_etl", "data_engineers", "data_stewards"],
}


def _catalog_and_schema_grants():
    catalog_grants = {}      # principal -> [privileges]
    schema_grants = {}       # (principal, schema) -> set(privileges)

    for grant in G.build_grants():
        if grant["securable_type"] == "CATALOG":
            catalog_grants.setdefault(grant["principal"], set()).add(grant["privilege"])
        elif grant["securable_type"] == "SCHEMA":
            # securable like "investsphere.silver_cdc" -> short schema name
            schema = grant["securable"].split(".", 1)[1]
            schema_grants.setdefault((grant["principal"], schema), set()).add(
                grant["privilege"])

    # augment infra-only schemas
    for schema, principals in INFRA_SCHEMA_EXTRA_GRANTS.items():
        for principal in principals:
            schema_grants.setdefault((principal, schema), set()).add("USE SCHEMA")

    catalog_out = {p: sorted(v) for p, v in catalog_grants.items()}
    schema_out = [
        {"principal": p, "schema": s, "privileges": sorted(v)}
        for (p, s), v in sorted(schema_grants.items())
    ]
    return catalog_out, schema_out


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    catalog_grants, schema_grants = _catalog_and_schema_grants()

    schemas_path = os.path.join(OUT_DIR, "schemas.auto.tfvars.json")
    grants_path = os.path.join(OUT_DIR, "grants.auto.tfvars.json")

    with open(schemas_path, "w", encoding="utf-8") as fh:
        json.dump({"schemas": INFRA_SCHEMAS}, fh, indent=2)
        fh.write("\n")
    with open(grants_path, "w", encoding="utf-8") as fh:
        json.dump({"catalog_grants": catalog_grants,
                   "schema_grants": schema_grants}, fh, indent=2)
        fh.write("\n")

    print("wrote", os.path.relpath(schemas_path, _ROOT))
    print("wrote", os.path.relpath(grants_path, _ROOT))
    print("%d schemas, %d schema-grants, %d principals with catalog grants"
          % (len(INFRA_SCHEMAS), len(schema_grants), len(catalog_grants)))


if __name__ == "__main__":
    main()
