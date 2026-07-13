"""
Optional MCP server for the InvestSphere business tools.

This is an **interoperability layer** for MCP-compatible hosts (Claude Desktop,
IDEs, other agent frameworks). It is NOT part of the production Azure runtime — the
Foundry / FastAPI OpenAPI tools remain the deployed path. This server simply exposes
the SAME governed, read-only tools over the Model Context Protocol by reusing
`ai.mcp.tools` (which reuses `business_tools.TOOL_DISPATCH`). No logic is duplicated.

Run over stdio (the usual MCP transport):

    python -m ai.mcp.server

Requires the `mcp` SDK (`pip install mcp`) at runtime and the same env the business
tools need at CALL time (Databricks SQL Warehouse token, Azure Search creds). It
imports the SDK at module top on purpose — the credential-free wiring check targets
`ai.mcp.tools`, not this runtime module.
"""
from __future__ import annotations

import asyncio
import json

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from ai.mcp import tools as registry

server = Server("investsphere-enterprise")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(name=t["name"], description=t["description"], inputSchema=t["inputSchema"])
        for t in registry.list_tools()
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict | None = None) -> list[TextContent]:
    result = registry.call_tool(name, arguments or {})
    return [TextContent(type="text", text=json.dumps(result, default=str))]


async def _main() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
