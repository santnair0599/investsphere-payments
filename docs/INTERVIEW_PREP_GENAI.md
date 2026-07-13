# GenAI Interview Prep — concepts to master (project-grounded)

Since you'll be defending *this* project, each concept below is tied to something you
actually built + a likely question. Tiered by how certain you are to be asked.

## Tier 1 — you *will* be asked (core)

### 1. RAG (Retrieval-Augmented Generation)
- **Know:** retrieve → augment prompt → generate; chunking, embeddings, top-k, **hybrid
  search (keyword+vector)**, semantic reranking; **why RAG beats fine-tuning for facts**
  (updatable, cited, no hallucinated numbers).
- **Project:** Azure AI Search over policy docs (`ai/rag/`), hybrid + semantic.
- **Q:** *"How do you stop the model hallucinating a payment total?"* → "It doesn't recall —
  it calls a SQL tool over the Gold mart and RAGs policy; answers are grounded + cited."

### 2. Agents & tool/function calling
- **Know:** the tool-calling loop (model emits tool call → you execute → feed result back →
  synthesize), **ReAct**, structured tools vs free NL2SQL, tool schemas/descriptions as the
  "teaching," multi-turn.
- **Project:** LangGraph/raw agent, **UC functions as governed tools** over Gold marts,
  `search_policy_docs` retriever tool.
- **Q:** *"Why UC functions instead of letting the LLM write SQL?"* → governed, testable,
  safe (read-only, no raw PII).

### 3. Grounding vs hallucination
- **Know:** grounding = answer supported by retrieved/queried evidence; how you *measure* it
  (groundedness score); citations.
- **Q:** *"How do you know the answer is grounded?"* → groundedness evaluator + the answer
  cites the query rows + policy.

### 4. Evaluation / GenAIOps (a differentiator — know it cold)
- **Know:** **golden dataset**, **LLM-as-judge**, metrics (groundedness, relevance,
  **tool-call accuracy**, task adherence), offline vs online eval, **eval gates in CI**.
- **Project:** `ai/eval/run_evals.py` + Azure AI Evaluation, the quality gate, nightly live gate.
- **Q:** *"How do you evaluate an agent, not just an LLM?"* → tool-call accuracy + task
  adherence, not just answer text.

### 5. Prompt engineering & structured outputs
- **Know:** system vs user prompts, few-shot, **JSON mode / structured output**,
  schema-constrained responses, prompt-injection basics.
- **Project:** `BusinessRecommendation` pydantic schema (`ai/app/schemas.py`),
  `RECOMMENDATION_JSON_SCHEMA`.

### 6. Embeddings & vector search
- **Know:** embeddings = semantic vectors; cosine similarity; vector index (HNSW); why
  "single-payment limit" ≈ "max I can pay"; embedding model choice (multilingual for Arabic).
- **Project:** `text-embedding-3-small`, Azure AI Search vector index.

## Tier 2 — strongly expected for this project
- **7. Model selection & routing** — criteria (tool-calling, groundedness, latency, cost),
  **two-tier GPT-4o + GPT-4o-mini router**, the bake-off. (Rehearse `docs/MODEL_SELECTION.md`.)
- **8. Guardrails & Responsible AI** — Content Safety, **Prompt Shields**, PII
  detection/masking, jailbreak, **red-teaming / attack success rate** (`ai/redteam/`, `ai/guardrails/`).
- **9. Observability & tracing** — spans (`agent.run`/`llm.chat`/`tool.call`), App Insights,
  **token/cost tracking**, inference tables.
- **10. Serving & the AI Gateway** — **external models** (Databricks endpoint → Azure OpenAI
  GPT-4o), why route through a **gateway** (one control plane: auth, rate limits, usage),
  Model Serving, Container Apps.
- **11. LLMOps / CI-CD for AI** — eval gate blocks merge, build-once/promote-by-digest,
  blue-green + canary, nightly regression (`docs/CICD_SETUP.md`).

## Tier 3 — differentiators (UAE / Azure Foundry)
- **Azure AI Foundry specifics:** Foundry projects, **Agent Service**, deployments
  (**Global Standard**), Azure OpenAI, **AI Search skillsets**, **Document Intelligence / OCR** (`ai/docintel/`).
- **Arabic / multilingual:** cross-lingual embeddings, **retrieval + answer parity** (`ai/i18n/`).
- **Data governance & residency:** Unity Catalog masked views, PII, **UAE data sovereignty**,
  managed-identity auth (no PAT).
- **Human-in-the-loop:** approval gating for sensitive actions (`ai/ui/`).

## Fundamentals you must not fumble
- **Tokens, context window, temperature, top-p** — and how they affect cost/latency/determinism.
- **RAG vs fine-tuning vs prompt engineering** — when each; *"we don't fine-tune — accuracy
  comes from grounding; fine-tuning is for style/format, not facts."*
- **Cost & latency levers** — model routing, prompt caching, token budgets, `LIMIT` on tool results.
- **Why the agent won't leak data** — read-only, masked views, guardrails, no raw PII.

## The 8 questions to rehearse out loud
1. Walk me through your architecture end-to-end. *(medallion → Gold → agent → tools/RAG → eval → serve)*
2. How do you prevent hallucination / ensure groundedness?
3. RAG vs fine-tuning — why RAG?
4. How did you pick your model, and how would you swap it?
5. How do you evaluate the agent (not just the LLM)?
6. How do you handle PII / prompt injection / jailbreaks?
7. How does a bad model version get blocked from production?
8. What's Azure-native vs where does Databricks fit? *(data in Databricks, AI in Foundry)*

---
**Two things that make you stand out:** *"we don't fine-tune, we ground and evaluate,"* and
*"the model is a swappable backend behind an AI Gateway, chosen by a benchmark, not a vibe."*
Both are true of this project and rare from candidates.

See also: [MODEL_SELECTION.md](MODEL_SELECTION.md) · [RAG_RUNBOOK.md](RAG_RUNBOOK.md) ·
[EVALUATION_OBSERVABILITY.md](EVALUATION_OBSERVABILITY.md) · [GENAI_INTERVIEW_STORY.md](GENAI_INTERVIEW_STORY.md).
