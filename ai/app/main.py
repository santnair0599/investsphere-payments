"""
FastAPI surface for the Enterprise Decision Agent (deployed to Azure Container Apps).

Endpoints:
  GET  /health          — liveness
  GET  /tools           — list the agent's tool surface (marts + RAG)
  POST /ask             — ask a business question, get a grounded answer + trace
  POST /recommend       — same, but returns a schema-constrained BusinessRecommendation (JSON)
  GET  /runs/{run_id}   — fetch a traced run (audit)

Auth in front is handled by Azure API Management (token metering, rate limiting);
this app trusts the gateway and focuses on the agent logic.
"""
from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel, Field

from ai.app.runtime import answer          # selects raw or langgraph via AGENT_RUNTIME
from ai.app.agent import ALL_TOOL_SPECS
from ai.app.tools_router import router as tools_router
from ai.app.schemas import BusinessRecommendation

app = FastAPI(title="InvestSphere Enterprise Decision Agent", version="1.0.0")

# Per-tool REST operations (POST /tools/<tool_name>) consumed by the Azure AI
# Foundry OpenAPI tool. See ai/foundry/openapi_tools.json.
app.include_router(tools_router)


class AskRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=2000)
    session_id: str = "adhoc"
    user_id: str = "anonymous"


class AskResponse(BaseModel):
    question: str
    answer: str
    tools_called: list[str]
    retrieved_docs: list[str | None] = []
    safety_status: str
    latency_ms: int


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/tools")
def tools():
    return {"tools": [t["function"]["name"] for t in ALL_TOOL_SPECS]}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    result = answer(req.question, session_id=req.session_id, user_id=req.user_id)
    return AskResponse(
        question=result["question"],
        answer=result["answer"],
        tools_called=result.get("tools_called", []),
        retrieved_docs=result.get("retrieved_docs", []),
        safety_status=result.get("safety_status", "PASS"),
        latency_ms=result.get("latency_ms", 0),
    )


@app.post("/recommend", response_model=BusinessRecommendation)
def recommend(req: AskRequest):
    """Grounded answer coerced into the structured BusinessRecommendation schema (JSON)."""
    from ai.app.structured import answer_structured
    out = answer_structured(req.question, session_id=req.session_id, user_id=req.user_id)
    return out["recommendation"]
