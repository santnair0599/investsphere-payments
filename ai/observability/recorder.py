"""
Best-effort tracer: writes agent turns to ai_control.agent_runs / agent_tool_calls
on the Databricks SQL Warehouse. Never raises into the request path (the agent wraps
calls in try/except) — observability must not break the product.
"""
from __future__ import annotations

import json
import os
import uuid

from ai.tools.databricks_client import WarehouseConfig


def _connect():
    from databricks import sql as dbsql
    cfg = WarehouseConfig.from_env()
    return dbsql.connect(server_hostname=cfg.server_hostname, http_path=cfg.http_path,
                         access_token=cfg.access_token, catalog=cfg.catalog)


def record_run(record: dict, tool_trace: list[dict]) -> str:
    """Insert one agent_runs row + one agent_tool_calls row per tool. Returns run_id."""
    run_id = str(uuid.uuid4())
    catalog = os.environ.get("DATABRICKS_CATALOG", "investsphere_prod")
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {catalog}.ai_control.agent_runs
                (run_id, session_id, user_id, question, answer, tools_called,
                 retrieved_docs, safety_status, latency_ms,
                 model, routing_reason, tokens_in, tokens_out, estimated_cost,
                 groundedness_score, hallucination_flag, evaluation_mode, created_at)
                VALUES (:run_id, :session_id, :user_id, :question, :answer,
                        :tools, :docs, :safety, :latency,
                        :model, :routing, :tin, :tout, :cost,
                        :ground, :halluc, :evalmode, current_timestamp())""",
            {"run_id": run_id,
             "session_id": record.get("session_id"),
             "user_id": record.get("user_id"),
             "question": record.get("question"),
             "answer": record.get("answer"),
             "tools": json.dumps(record.get("tools_called", [])),
             "docs": json.dumps(record.get("retrieved_docs", [])),
             "safety": record.get("safety_status"),
             "latency": record.get("latency_ms"),
             "model": record.get("model"),
             "routing": record.get("routing_reason"),
             "tin": record.get("tokens_in", 0),
             "tout": record.get("tokens_out", 0),
             "cost": record.get("estimated_cost", 0),
             "ground": record.get("groundedness_score"),
             "halluc": record.get("hallucination_flag"),
             "evalmode": record.get("evaluation_mode")},
        )
        for t in tool_trace:
            cur.execute(
                f"""INSERT INTO {catalog}.ai_control.agent_tool_calls
                    (run_id, tool_name, arguments, mart, success, created_at)
                    VALUES (:run_id, :tool, :args, :mart, :ok, current_timestamp())""",
                {"run_id": run_id, "tool": t.get("tool"),
                 "args": json.dumps(t.get("args", {})), "mart": t.get("mart"),
                 "ok": bool(t.get("ok"))},
            )
    return run_id
