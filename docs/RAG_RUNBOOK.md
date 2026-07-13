# RAG Runbook — end-to-end build sequence

RAG is a *pipeline* and the order matters. This is the actual sequence for this project,
blending the Azure Foundry portal steps with the code steps, in the order you'd do them.

## Phase A — Foundry setup & model deployment (portal)
1. **Create the Foundry hub + project** (`ai.azure.com`) — the workspace for models,
   connections, and evals.
2. **Deploy the chat model** — GPT-4o, **deployment type = Global Standard** (dodges the
   regional quota wall), name it e.g. `gpt-4o`.
3. **Deploy the embedding model** — `text-embedding-3-small` (RAG needs an embedder,
   separate from the chat model).
4. **Test the chat model in the Playground** — confirm it responds, tool-calls, and returns
   JSON. Your "is the model alive and behaving" check *before* building around it.
5. **Provision RAG dependencies + connections** — **Azure AI Search** (vector store),
   **Content Safety**, **App Insights** — add them as **connections** in the Foundry project.

## Phase B — Build the knowledge index (code) → `ai/rag/`
6. **Assemble the corpus** — policy/KPI markdown (`ai/rag/policies/*.md`, incl. the Arabic set).
7. **Chunk → embed → create the index** — `python -m ai.rag.index_policies`:
   - splits each doc into chunks (~1200 chars),
   - calls the **embedding deployment** to vectorize each chunk,
   - creates a **hybrid + semantic index** in Azure AI Search (BM25 keyword field +
     `content_vector` HNSW field + a semantic-ranker config),
   - uploads the chunks. (Idempotent — re-run after editing a policy.)
8. **Verify in the Azure AI Search portal** — open the index, run a search, confirm docs +
   vectors are populated.

## Phase C — Retrieval (code) → `ai/rag/retriever.py`
9. **Implement the retriever** — a query that does **hybrid** (keyword + vector) +
   **semantic reranking**, returns top-k chunks with `doc_id`.
10. **Test retrieval standalone** — query "what's the single-payment limit?" and confirm the
    right chunk comes back **before** wiring the LLM. *(Most "RAG is wrong" bugs are actually
    retrieval/chunking bugs — test this layer alone.)*

## Phase D — Connect retrieval to the model (two ways)
11. **Prototype fast in the Playground (low-code):** Foundry → **"Add your data" / Chat with
    your data** → point at the AI Search index → ask a question → grounded answers with
    citations. Great for a quick "does RAG work" demo.
12. **Productionize as a tool (this project):** expose retrieval as the agent's
    **`search_policy_docs`** tool (`ai/app/agent.py`), so the agent *decides* when to
    retrieve — alongside the SQL mart tools. The playground is just the prototype.
13. **Write the grounding system prompt** — use retrieved context, **cite the policy, refuse
    to invent** (`ai/app/system_prompt.md`).

## Phase E — Test & evaluate
14. **Test the full RAG loop** — ask policy questions in the agent (or playground): confirm
    the answer is **grounded + cites the policy**, not made up.
15. **Evaluate it** — `python -m ai.eval.run_evals` (groundedness, relevance, retrieval) +
    `python -m ai.benchmarks.foundry_iq_retrieval` (recall/precision/nDCG + lift). *Proves*
    RAG quality — not eyeballing.

## Phase F — Harden & deploy
16. **Guardrails** — Content Safety / Prompt Shields on the input path (`ai/guardrails/`).
17. **Deploy** — the index is (re)built in CI (`ai/rag/index_policies` runs in
    `ai-deploy.yml`); the agent that uses it is served on Container Apps.
18. **Observe** — tracing (`agent.run → tool.call` spans) + the retrieval benchmark in the
    nightly gate catch retrieval regressions.

---
## The mental model (say this in the interview)
- **Index-time (offline):** docs → chunk → embed → Azure AI Search hybrid index.
- **Query-time (per request):** question → embed → hybrid+semantic retrieve top-k → **stuff
  into the prompt as grounded context** → LLM answers with citations.
- The model is deployed and tested **first** (you need the chat + embedding deployments
  before you can build or query the index); RAG is wired **around** it as a **tool the agent
  calls**, not a hardcoded step.

**Ordering gotcha:** deploy the **embedding** model *before* Phase B (the index can't be
built without it), and test **retrieval alone (step 10)** before blaming the LLM.

Related: [INTERVIEW_PREP_GENAI.md](INTERVIEW_PREP_GENAI.md) · [MODEL_SELECTION.md](MODEL_SELECTION.md) ·
[EVALUATION_OBSERVABILITY.md](EVALUATION_OBSERVABILITY.md).
