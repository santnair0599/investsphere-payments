"""
Read-only Databricks SQL Warehouse client for the Azure GenAI plane.

The agent runs on Azure (Container Apps / Functions) and queries the **governed
Gold marts** — it never runs the pipeline. Connection details come from Azure Key
Vault via Managed Identity; the service principal has read-only grants on the
`gold_*` schemas and reads through Unity Catalog **masked views** (no raw PII).

Guardrails enforced here (defence in depth, independent of the LLM):
  * read-only: only SELECT statements are allowed to leave this module
  * bounded result size: every query is wrapped with a hard LIMIT
  * parameterised: callers pass typed params, never string-concatenated SQL
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass

# Hard ceiling on rows returned to the LLM context (cost + prompt-injection blast radius).
MAX_ROWS = 200

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|alter|create|grant|revoke|truncate|copy)\b",
    re.IGNORECASE,
)


DEFAULT_CATALOG = "investsphere_prod"


def catalog() -> str:
    """The Unity Catalog every AI-plane reader targets.

    Single source of truth: the recorder, the mart tools and the DocIntel
    reconciliation all resolve the catalog through here. They previously each
    inlined a default and disagreed — docintel fell back to `investsphere_dev`
    while everything else fell back to `investsphere_prod`, so an unset env var
    silently pointed one component at a different catalog than the rest.
    """
    return os.environ.get("DATABRICKS_CATALOG", DEFAULT_CATALOG)


@dataclass
class WarehouseConfig:
    server_hostname: str
    http_path: str
    access_token: str
    catalog: str = DEFAULT_CATALOG

    @classmethod
    def from_env(cls) -> "WarehouseConfig":
        """Load connection details. Auth is an Entra ID token minted via workload
        identity (managed identity on Container Apps, OIDC in CI, az-CLI locally) —
        no stored PAT. A `DATABRICKS_TOKEN` env var, if present, still wins for
        local/dev convenience."""
        return cls(
            server_hostname=os.environ["DATABRICKS_HOST"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ.get("DATABRICKS_TOKEN") or _aad_token(),
            catalog=catalog(),
        )


# Azure Databricks first-party app id — the resource for a Databricks-scoped token.
_DATABRICKS_AAD_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"


def _aad_token() -> str:
    """Short-lived Entra ID bearer for Databricks via DefaultAzureCredential.
    Nothing long-lived to store, leak, or rotate. The workspace must add the
    managed identity / OIDC service principal as a user with the read-only
    `gold_*` grants (SCIM)."""
    from azure.identity import DefaultAzureCredential
    cred = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    return cred.get_token(_DATABRICKS_AAD_SCOPE).token


def _guard(sql: str) -> None:
    if _FORBIDDEN.search(sql):
        raise ValueError("Only read-only SELECT queries are permitted by the agent.")
    if sql.count(";") > 0:
        raise ValueError("Multiple statements / trailing semicolons are not permitted.")


def run_query(sql: str, params: dict | None = None, limit: int = MAX_ROWS) -> list[dict]:
    """Execute a bounded, read-only, parameterised query and return list[dict].

    ``databricks-sql-connector`` is imported lazily so this module imports without
    the driver present (e.g. during static checks / schema validation).
    """
    _guard(sql)
    limit = min(int(limit), MAX_ROWS)
    wrapped = f"SELECT * FROM (\n{sql}\n) AS _agent_q LIMIT {limit}"

    from databricks import sql as dbsql  # lazy import

    cfg = WarehouseConfig.from_env()
    with dbsql.connect(
        server_hostname=cfg.server_hostname,
        http_path=cfg.http_path,
        access_token=cfg.access_token,
        catalog=cfg.catalog,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(wrapped, params or {})
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
