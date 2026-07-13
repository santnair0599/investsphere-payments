"""
REST surface for the business tools, so Azure AI Foundry Agent Service (and any
HTTP client) can call each governed-mart tool as an OpenAPI operation.

Foundry's `openapi` tool type invokes tools as HTTP operations against this app
(deployed to Azure Container Apps). Every tool in
``ai.tools.business_tools.TOOL_DISPATCH`` is exposed here as a single POST:

    POST /tools/get_underperforming_properties     body: {"period": "this_week"}
    POST /tools/get_pipeline_status                body: {"run_date": "2026-07-05"}
    ...
    GET  /tools/schemas                            -> the function-schema list

The operationId of each generated route == the tool name, which is exactly what
``ai/foundry/openapi_tools.json`` (and Foundry's tool registration) keys on. The
routes are derived from TOOL_SPECS so the REST surface can never drift from the
canonical tool list. Bodies are optional — omitting ``period``/``run_date`` uses
the tool's own default (e.g. latest run, ``this_week``).
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from ai.tools.business_tools import TOOL_DISPATCH, TOOL_SPECS, VALID_PERIODS

router = APIRouter(prefix="/tools", tags=["business-tools"])

# name -> function-schema (canonical param definitions)
_SPEC_BY_NAME = {s["function"]["name"]: s["function"] for s in TOOL_SPECS}


def _uses_run_date(name: str) -> bool:
    props = _SPEC_BY_NAME.get(name, {}).get("parameters", {}).get("properties", {})
    return "run_date" in props


class PeriodBody(BaseModel):
    """Reporting-window body for the domain-risk tools."""
    period: Optional[str] = Field(
        default=None,
        description=f"Reporting period window; one of {sorted(VALID_PERIODS)}. "
                    "Omit to use the tool default.",
        examples=["this_week"],
    )


class RunDateBody(BaseModel):
    """Run-date body for the ops-trust tools."""
    run_date: Optional[str] = Field(
        default=None,
        description="Pipeline run date as YYYY-MM-DD. Omit for the latest run.",
        examples=["2026-07-05"],
    )


def _make_period_endpoint(fn):
    def endpoint(body: PeriodBody = PeriodBody()) -> dict:
        kwargs = {} if body.period is None else {"period": body.period}
        return fn(**kwargs)
    return endpoint


def _make_run_date_endpoint(fn):
    def endpoint(body: RunDateBody = RunDateBody()) -> dict:
        kwargs = {} if body.run_date is None else {"run_date": body.run_date}
        return fn(**kwargs)
    return endpoint


# Register one POST route per tool, operationId == tool name.
for _name, _fn in TOOL_DISPATCH.items():
    _summary = _SPEC_BY_NAME.get(_name, {}).get("description", _name)
    if _uses_run_date(_name):
        _handler = _make_run_date_endpoint(_fn)
    else:
        _handler = _make_period_endpoint(_fn)
    router.add_api_route(
        f"/{_name}",
        _handler,
        methods=["POST"],
        name=_name,
        operation_id=_name,
        summary=_summary,
        response_model=dict,
    )


@router.get("/schemas", operation_id="get_tool_schemas", summary="List tool function-schemas")
def schemas() -> dict:
    """Return the OpenAI/Azure-AI-Foundry function-schema list for the marts tools."""
    return {"tools": TOOL_SPECS}
