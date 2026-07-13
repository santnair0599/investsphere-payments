"""
Generate ``ai/foundry/openapi_tools.json`` — the OpenAPI 3.0 document that Azure AI
Foundry Agent Service registers as the ``business_marts`` OpenAPI tool.

Foundry calls each tool as an HTTP operation against the FastAPI app's
``/tools/<tool_name>`` POST routes (see ``ai/app/tools_router.py``). This document
is derived from ``ai.tools.business_tools.TOOL_SPECS`` so the tool contract stays
in sync with the canonical tool list: one path per tool, ``operationId`` == tool
name, and the request-body schema mirrors each tool's parameters (the ``period``
enum, or a ``run_date`` string).

Import-safety: only imports ``business_tools`` (which imports ``databricks_client``
with a *lazy* driver import). No azure / openai / databricks SDKs are needed to
build the JSON.

Run from the repo root to (re)generate the file:

    python -m ai.foundry.build_openapi_tools
"""
from __future__ import annotations

import json
from pathlib import Path

from ai.tools.business_tools import TOOL_SPECS, VALID_PERIODS

OUTPUT = Path(__file__).parent / "openapi_tools.json"

# Replaced at deploy time with the Container App's ingress FQDN (Bicep output).
SERVER_URL = "https://REPLACE_WITH_CONTAINER_APP_FQDN"


def _request_schema(fn: dict) -> dict:
    """Build the JSON-Schema request body for one tool from its function spec."""
    props = fn.get("parameters", {}).get("properties", {})
    schema: dict = {"type": "object", "additionalProperties": False, "properties": {}}
    if "run_date" in props:
        schema["properties"]["run_date"] = {
            "type": "string",
            "description": "Pipeline run date as YYYY-MM-DD. Omit for the latest run.",
            "example": "2026-07-05",
        }
    else:
        schema["properties"]["period"] = {
            "type": "string",
            "enum": sorted(VALID_PERIODS),
            "description": "Reporting period window. Omit to use the tool default.",
            "example": "this_week",
        }
    return schema


def _response_schema() -> dict:
    """Every tool returns {mart, row_count, rows[...]} (or a cross-domain shape)."""
    return {
        "type": "object",
        "properties": {
            "mart": {"type": "string", "description": "Governed Gold mart the rows came from."},
            "row_count": {"type": "integer"},
            "rows": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
        },
        "additionalProperties": True,
    }


def build_openapi() -> dict:
    """Assemble the OpenAPI 3.0 document from TOOL_SPECS."""
    paths: dict = {}
    for spec in TOOL_SPECS:
        fn = spec["function"]
        name = fn["name"]
        paths[f"/tools/{name}"] = {
            "post": {
                "operationId": name,
                "summary": fn.get("description", name),
                "description": fn.get("description", name),
                "requestBody": {
                    "required": False,
                    "content": {
                        "application/json": {"schema": _request_schema(fn)}
                    },
                },
                "responses": {
                    "200": {
                        "description": "Structured rows from the governed Gold mart.",
                        "content": {
                            "application/json": {"schema": _response_schema()}
                        },
                    }
                },
                "security": [{"managed_identity": []}],
            }
        }

    return {
        "openapi": "3.0.3",
        "info": {
            "title": "InvestSphere Business Marts Tools",
            "version": "1.0.0",
            "description": (
                "Governed KPI/risk queries over Databricks Gold marts, exposed as "
                "OpenAPI operations for the Azure AI Foundry agent. Generated from "
                "ai/tools/business_tools.TOOL_SPECS — do not edit by hand; run "
                "`python -m ai.foundry.build_openapi_tools`."
            ),
        },
        "servers": [{"url": SERVER_URL, "description": "Azure Container App ingress (FastAPI)."}],
        "paths": paths,
        "components": {
            "securitySchemes": {
                "managed_identity": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": (
                        "Azure Managed Identity token issued to the Foundry agent's "
                        "OpenAPI tool connection; validated by API Management / the "
                        "Container App ingress. No API key is stored in this document."
                    ),
                }
            }
        },
        "security": [{"managed_identity": []}],
    }


def main() -> None:
    doc = build_openapi()
    OUTPUT.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUTPUT} ({len(doc['paths'])} tool paths)")


if __name__ == "__main__":
    main()
