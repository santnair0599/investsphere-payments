# AI enhancements 6–12 (Azure Foundry GenAI)

Added to the existing `ai/` plane. All are **feature-flagged** — each runs offline
with a deterministic fallback (so CI/dry-runs work), and flips to the real Azure
service when its env flag + creds are set (see `ai/.env.example`).

| # | Capability | Entry point | Offline run | UAE JD skill |
|---|-----------|-------------|-------------|--------------|
| 6 | **Foundry IQ agentic-retrieval benchmark** | `ai/benchmarks/foundry_iq_retrieval.py` | `python -m ai.benchmarks.foundry_iq_retrieval --dry-run` | agentic RAG, retrieval eval |
| 7 | **Document Intelligence reconciliation** | `ai/docintel/reconcile.py` | `python -m ai.docintel.reconcile --dry-run` | OCR / doc AI, finance ops |
| 8 | **Production failure + load tests** | `ai/tests/load/locustfile.py`, `ai/tests/failure/chaos.py` | `pytest ai/tests/failure/chaos.py` | SRE/LLMOps, resilience, SLOs |
| 9 | **Responsible-AI red-team** | `ai/redteam/redteam_suite.py` | `python -m ai.redteam.redteam_suite` | responsible AI, security |
| 10 | **Arabic bilingual retrieval + parity** | `ai/i18n/arabic_parity.py` | `python -m ai.i18n.arabic_parity` | Arabic NLP (UAE-critical) |
| 11 | **Streaming approval UI (HITL)** | `ai/ui/streaming.py` + `index.html` | include router → open `/ui` | agentic UX, human-in-the-loop |
| 12 | **Teams publishing** | `ai/integrations/teams/publish.py` | `python -m ai.integrations.teams.publish --demo` | enterprise integration |

## How each plugs in
- **6** compares Azure AI Search **agentic retrieval** (query decomposition → parallel
  sub-queries → merge) vs the baseline hybrid retriever; reports recall/precision/MRR/nDCG lift.
- **7** extracts settlement/invoice PDFs with Document Intelligence and reconciles them
  against `gold.fact_payments` → MATCH / AMOUNT_MISMATCH / MISSING_IN_LEDGER / MISSING_IN_DOC.
  Also usable as an agent tool.
- **8** — `locust` load test with an SLO gate (p95 < 4s, err < 1%) + `pytest` chaos tests
  that inject warehouse/search/LLM failures and assert graceful degradation.
- **9** runs curated + (optional) auto-generated adversarial attacks and reports **ASR**
  per category (jailbreak, prompt-injection, PII exfil, data leakage, toxicity, over-refusal);
  non-zero breaches fail CI.
- **10** adds Arabic policy docs (`ai/rag/policies/ar/`) and asserts AR/EN **retrieval + answer
  parity** (same figures, correct language). Use `text-embedding-3-large` for cross-lingual embeddings.
- **11** — SSE streaming answers; sensitive **actions pause for approval** (`/approve/{id}`),
  matching the MCP action gate. Include the router in `ai/app/main.py`:
  ```python
  from ai.ui.streaming import router as ui_router
  app.include_router(ui_router)
  ```
- **12** posts recommendations / monitoring alerts / answers to Teams as Adaptive Cards.

## Suggested CI order (all offline, deterministic)
```
pytest ai/tests/failure/chaos.py
python -m ai.redteam.redteam_suite          # exit 1 on any breach
python -m ai.i18n.arabic_parity             # parity gate
python -m ai.benchmarks.foundry_iq_retrieval --dry-run
python -m ai.docintel.reconcile --dry-run
```
