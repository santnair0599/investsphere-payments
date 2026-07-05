"""
Generate the BI serving-view SQL + the Power BI measure catalogue.

    python pipelines/generate_bi_sql.py [catalog]

Writes:
  bi/sql/*.sql        CREATE OR REPLACE VIEW + analyst grants (run after governance SQL)
  bi/measures.json    measures / dimensions / datasets / RLS for the Power BI model

Validates the semantic model first (fails on any violation), so generated SQL can
never expose PII or reference a missing column.
"""
from __future__ import annotations

import json
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))

from payments_platform.bi import sql_generator, semantic, validate  # noqa: E402

OUT_DIR = os.path.join(_ROOT, "bi", "sql")
CATALOG_FILE = os.path.join(_ROOT, "bi", "measures.json")


def main(catalog="investsphere"):
    violations = validate.validate_all()
    if violations:
        print("BI MODEL INVALID:")
        for v in violations:
            print("  -", v)
        raise SystemExit(1)

    os.makedirs(OUT_DIR, exist_ok=True)
    files = sql_generator.generate_all(catalog=catalog)
    for filename, sql in files.items():
        path = os.path.join(OUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(sql)
        print("wrote", os.path.relpath(path, _ROOT), "(%d bytes)" % len(sql))

    with open(CATALOG_FILE, "w", encoding="utf-8") as fh:
        json.dump(semantic.measure_catalog(), fh, indent=2)
        fh.write("\n")
    print("wrote", os.path.relpath(CATALOG_FILE, _ROOT))
    print("\n%d BI serving views + measure catalogue generated for catalog '%s'."
          % (len(files), catalog))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "investsphere")
