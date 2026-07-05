"""
Generate the Databricks SQL dashboard queries from the monitoring model.

    python pipelines/generate_monitoring_sql.py [catalog]

Writes one .sql file per dashboard to monitoring/sql/. Run them against the
SQL warehouse (or paste into a Databricks SQL dashboard) for the per-env catalog
(default: investsphere; pass e.g. investsphere_prod to target prod).
"""
from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))

from payments_platform.monitoring import dashboards  # noqa: E402

OUT_DIR = os.path.join(_ROOT, "monitoring", "sql")


def main(catalog="investsphere"):
    os.makedirs(OUT_DIR, exist_ok=True)
    files = dashboards.generate_all(catalog=catalog)
    for filename, sql in files.items():
        path = os.path.join(OUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(sql)
        print("wrote", os.path.relpath(path, _ROOT), "(%d bytes)" % len(sql))
    print("\n%d dashboard SQL files generated for catalog '%s'."
          % (len(files), catalog))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "investsphere")
