"""
Generate the Databricks Unity Catalog governance SQL from the policy model.

    python pipelines/generate_governance_sql.py

Writes ordered SQL files to governance/sql/. Run these in order against the
workspace (Databricks SQL / notebook / `databricks sql`), after the groups exist
(provisioned via SCIM/Terraform).
"""
from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))

from payments_platform.governance import sql_generator  # noqa: E402

OUT_DIR = os.path.join(_ROOT, "governance", "sql")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    files = sql_generator.generate_all()
    for filename, sql in files.items():
        path = os.path.join(OUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(sql)
        print("wrote", os.path.relpath(path, _ROOT), "(%d bytes)" % len(sql))
    print("\n%d governance SQL files generated." % len(files))


if __name__ == "__main__":
    main()
