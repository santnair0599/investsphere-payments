"""
[11] Streaming answer UI with human-in-the-loop approval.

A FastAPI router (include it in ai/app/main.py) that:
  * GET  /ui                       -> serves the streaming chat page
  * POST /ask/stream               -> Server-Sent Events: token deltas as they arrive
  * emits an `approval_required` SSE event when the agent proposes a GATED action
    (e.g. an MCP write/action tool) — the answer pauses instead of executing
  * POST /approve/{action_id}      -> human approves/rejects; approved actions run

Read-only Q&A streams straight through. Only *actions* (state-changing tools) hit
the approval gate, matching ai/mcp/ (MCP_ENABLE_ACTIONS stays approval-gated).

  from ai.ui.streaming import router as ui_router
  app.include_router(ui_router)
"""
from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

from fastapi import APIRouter, Body
from fastapi.responses import StreamingResponse, HTMLResponse

router = APIRouter()
_PAGE = (Path(__file__).parent / "index.html")

# action_id -> {"tool":..., "args":..., "event": asyncio.Event, "decision": bool|None}
PENDING: dict[str, dict] = {}


@router.get("/ui", response_class=HTMLResponse)
def ui():
    return _PAGE.read_text(encoding="utf-8")


def _sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


async def _stream_answer(question: str, session_id: str):
    """Yield SSE frames. Uses the runtime's streaming hook if present, else chunks
    the final answer. Gated actions raise an approval event and wait."""
    yield _sse("start", {"question": question})
    try:
        from ai.app.runtime import answer_stream            # optional async generator
        agen = answer_stream(question=question, session_id=session_id)
    except Exception:
        agen = None

    if agen is not None:
        async for chunk in agen:
            if chunk.get("type") == "action_proposal":
                async for frame in _await_approval(chunk):
                    yield frame
            else:
                yield _sse("token", {"text": chunk.get("text", "")})
        yield _sse("done", {})
        return

    # fallback: non-streaming runtime -> chunk the answer, gate any proposed action
    from ai.app.runtime import answer
    result = answer(question=question, session_id=session_id) or {}
    action = result.get("proposed_action")
    if action:
        async for frame in _await_approval(action):
            yield frame
        if PENDING.get(action["action_id"], {}).get("decision") is not True:
            yield _sse("done", {"status": "rejected"})
            return
    for word in (result.get("answer") or "").split(" "):
        yield _sse("token", {"text": word + " "})
        await asyncio.sleep(0.02)
    yield _sse("trace", {"tools_called": result.get("tools_called", [])})
    yield _sse("done", {"status": "ok"})


async def _await_approval(action):
    action_id = action.get("action_id") or uuid.uuid4().hex
    ev = asyncio.Event()
    PENDING[action_id] = {"tool": action.get("tool"), "args": action.get("args"),
                          "event": ev, "decision": None}
    yield _sse("approval_required", {"action_id": action_id,
               "tool": action.get("tool"), "args": action.get("args")})
    try:
        await asyncio.wait_for(ev.wait(), timeout=120)
    except asyncio.TimeoutError:
        PENDING[action_id]["decision"] = False
        yield _sse("approval_timeout", {"action_id": action_id})
        return
    approved = PENDING[action_id]["decision"] is True
    yield _sse("approval_result", {"action_id": action_id, "approved": approved})


@router.post("/ask/stream")
async def ask_stream(payload: dict = Body(...)):
    q = (payload or {}).get("question", "")
    sid = (payload or {}).get("session_id", "ui")
    return StreamingResponse(_stream_answer(q, sid), media_type="text/event-stream")


@router.post("/approve/{action_id}")
def approve(action_id: str, payload: dict = Body(...)):
    p = PENDING.get(action_id)
    if not p:
        return {"error": "unknown or expired action_id"}
    p["decision"] = bool((payload or {}).get("approve"))
    p["event"].set()
    return {"action_id": action_id, "approved": p["decision"]}
