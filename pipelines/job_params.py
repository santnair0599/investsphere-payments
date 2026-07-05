"""
Read a parameter from a Databricks job/widget, a CLI flag, or a default.

    RUN_ID = get_param("run_id", "local-run")

Priority: Databricks widget/job parameter -> CLI `--name value` -> default.
Works off-Databricks (the dbutils import simply fails and is skipped).
"""
from __future__ import annotations

import sys


def get_param(name, default):
    # 1. Databricks job/widget parameter
    try:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        value = dbutils.widgets.get(name)
        if value:
            return value
    except Exception:
        pass

    # 2. CLI flag --name value
    flag = "--" + name
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]

    # 3. default
    return default
