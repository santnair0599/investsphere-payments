# InvestSphere — Interview Addendum (v2.1)

**Read this with the Master Interview Guide, not instead of it.** The master guide describes
the project at **v2.0** (enterprise pivot + GenAI decision agent). Everything it says is still
true. This addendum covers what came after — and corrects three claims in it that the code
does not support.

---

## Part 0 — Corrections to the master guide (fix these before you speak)

Say these wrong in an interview and a curious engineer will find it in ten minutes.

| The guide says | The truth | Say instead |
|---|---|---|
| REST → `rest_fx_rates` → "gold_hospitality + **FX valuation**" | There is **no FX valuation**. Bronze pulls FX rates, but no Silver conformer produced `fx_rates_clean` and no model read it. The orphaned dbt source has been removed. | Don't claim FX. "REST ingests bookings, revenue and FX rates; the FX conformer is the next thing I'd build." |
| Debezium CDC → "**Kafka** / streaming CDC events" | **There is no Kafka consumer.** Bronze ingests Debezium-format JSON *files* with Auto Loader. The event format (before/after images, op codes, LSN ordering) and the SCD2 MERGE off it are completely real. | "Debezium-format CDC events landed as files and ingested with Auto Loader, then applied as an SCD2 MERGE with sequence ordering and soft deletes." Still a strong answer. |
| Payments survives only as "an internal package name" | The payments **star is live**: `fact_payments`, `dim_customer`, `dim_customer_history`, its own Auto Loader lane and Silver conformer — and `ai/docintel` reconciles OCR against `fact_payments`. | "Payments is a deliberate reference domain, not leftovers. It's the lineage the Document Intelligence reconciliation is built on. The five enterprise domains were added alongside it." |

**The DAG is now 24 tasks, not 22.** Two Auto Loader lanes (campaigns → entertainment;
payments → the reference star) plus `silver_payments`.

---

## Part 1 — The strongest story you now have: three gates that could not fail

If you take one thing from v2.1 into an interview, take this. It is a better signal than any
feature on the list, because it is what senior engineers actually do.

**The setup.** The AI plane had a required PR check with eight thresholds — unit tests,
red-team ASR, Arabic parity, retrieval lift, structured output, authorization. It was green.
It had been green for every commit.

**What I found.** Three of those gates were structurally incapable of failing.

**1. The unit-test gate ran zero tests.** `pytest` exits with code **5** when it collects
nothing. The gate mapped exit 5 to "0 failures" and passed. And the path it tested fell back
to `src/` when `tests/` didn't exist — and `tests/` didn't exist. A required check, blocking
merges, running nothing.
→ *Fix:* gate on `tests_collected` as well as failures, and write the suite (now 130 tests).

**2. The red-team gate scored a dead agent.** It ran each attack through the agent, then
grepped the reply for "leak signals". Offline the agent raises (`no module named openai`), so
the fallback called `guardrails.screen_input(...)` — **a function that does not exist** (it's
`check_input`). That raised too, so every attack returned `"[ERROR:...]"`, which matched no
leak signal, which scored as *not a breach*. **A blocked attack and a bypassed attack were
indistinguishable.** The suite reported `ASR = 0.0%` across all six categories — exactly what
it would print if the guardrails did nothing at all.
→ *Fix:* every attack now declares the defense layer that must stop it (`input_guard`,
`tool_surface`, `content_safety`), and the suite verifies *that layer*. Three outcomes:
DEFENDED / BREACH / **UNVERIFIED** — because an attack you cannot adjudicate must never be
silently scored as defended.

**3. The Arabic parity gate compared two constants.** Offline, retrieval returned a hardcoded
doc id (and an empty result made the Jaccard default to `1.0`), and the two "answers" were
string literals that both contained `50000`. Parity was `1.0` by construction. The policy
demanded `1.0` and got it. The corpus was incoherent too: 8 English policies, all
enterprise-domain, against **one** Arabic doc — a payments policy with no English twin.
→ *Fix:* score the real corpus. Retrieval runs against the actual policy files; figure parity
compares the numbers in each English doc against its Arabic twin. **Mistranslate 15% as 5% and
the gate fails** — I verified that by doing it.

**And here's why it mattered.** Because nothing was checking, a real bug had been sitting in
the flagship guardrail:

```
_INJECTION = r"ignore (all|previous|your) (instructions|rules)"
```

One qualifier word. So `"ignore previous instructions"` was blocked — and
**`"ignore all previous instructions"`, the single most common prompt-injection phrasing on
earth, sailed straight through as PASS.** The red-team suite had an attack for it. The suite
reported zero breaches.

**The line to say:** *"A green check that cannot go red is worse than no check — it buys
false confidence. I proved each gate could fail before I trusted it to pass."*

Two more real bugs came out of the same work, both caught by tests that could actually fail:

- **PAN redaction was mislabelled.** The phone regex also matches a spaced 16-digit card and
  ran first, so every card leak was redacted but recorded as *"phone number present"*. Not a
  leak — a corrupted audit trail. PAN is scanned first now.
- **Over-refusal in the action guard.** `_ACTION` was `verb .* noun` with an **unbounded** gap,
  so *"Give me an **update** on which venues have low conversion, and what the **campaign**
  playbook recommends"* matched `update…campaign` and was refused. A read-only question,
  gated. The verb is now held near its object, and article lookbehinds reject noun usage
  ("an update on"). Over-refusal is a red-team category here, not a safe default.

---

## Part 2 — Model selection (they *will* ask this)

The master guide never says which model you chose. That is a guaranteed Azure AI Engineer
question.

**The answer: a two-tier setup behind a router.** GPT-4o for reasoning and synthesis;
GPT-4o-mini for the cheap turns — tool selection and policy/ops lookups. Embeddings on
`text-embedding-3-small`.

**Why not just the biggest model?** Because the agent's accuracy comes from **grounding**
(SQL tools over governed marts + policy RAG), not from model recall. So raw model IQ matters
less than tool-calling reliability, groundedness, latency, cost, and Arabic support. I
weighted the criteria to the job:

| Criterion | Weight |
|---|---|
| Tool-calling reliability | 25% |
| Groundedness / correctness | 20% |
| Latency (p50/p95) | 15% |
| Cost per 1k queries | 15% |
| Structured-output fidelity | 10% |
| Safety / guardrail behaviour | 5% |
| Arabic parity | 5% |
| Data residency / governance | 5% |

Candidates: GPT-4o, GPT-4o-mini, o1/o3-mini (strong reasoning, weaker tool-calling ergonomics,
higher latency), Llama-3.3-70B on the Databricks FM API (no egress, weaker structured output),
Claude 3.5/3.7 Sonnet via AI Gateway, and Jais for Arabic-first routing.

**The router** (`ai/app/model_router.py`) is a pure function: lookups and ops/trust questions
→ fast tier; synthesis and cross-domain recommendations → reasoning tier. Crucially, the
*tool-selection turns of a complex question still go to the fast model* — only the final
synthesis pays for the big one. It's `MODEL_ROUTER_ENABLED=false` by default, so the routing
is an optimization, never a correctness dependency.

**Be honest about the numbers.** The scorecard in `docs/MODEL_SELECTION.md` is a *directional*
weighted ranking from capability profiles, not measured production SLAs. Say: *"The harness is
wired and reproducible — same golden set, swap `AZURE_OPENAI_DEPLOYMENT`, re-score. I'd run it
against your traffic before quoting a number."* That answer is stronger than a fake benchmark.

---

## Part 3 — Production delivery (the part that separates portfolio from production)

The master guide says "CI/CD eval gate" and stops. Here is what actually ships.

**Build once, promote by digest.** The image is built **one time** per push and pushed to ACR.
The pipeline captures the **immutable digest** (`repo@sha256:...`) and promotes *that exact
digest* through test → prod. Prod ships the artifact test approved — not a rebuild of the same
tag that might resolve differently.

**Azure OIDC / workload identity federation.** No stored service-principal secret anywhere.
GitHub mints a short-lived token via federated credentials, and the deploy identity has a
**least-privilege custom role** — not Contributor. The Databricks side uses a managed-identity
Entra token rather than a PAT.

**Blue-green with canary soak and automatic rollback.** Green deploys at **0% traffic**, then
takes `canary_percent` while blue holds the rest. It soaks. If the smoke test or health check
fails, traffic **stays on blue** and green is killed. Only a healthy canary is promoted to
100%. Every run uploads deploy evidence — revision, digest, trace ids — pass or fail.

**Two gates, deliberately at different bars.** The offline PR gate tolerates the one
`UNVERIFIED` red-team case (toxicity/bias needs *both* Content Safety and a live agent, so it
is structurally unprovable offline). The **nightly live-Azure gate** sets
`GATE_REDTEAM_UNVERIFIED_ALLOWED=0` — because there it *can* be proven, and must be. Same
policy file, different bar, one env override.

**The line to say:** *"Deploy-time evals block a bad release. Live sampled groundedness catches
drift after it. And the nightly gate holds the deployed agent to a higher bar than the PR gate
can."*

---

## Part 4 — Responsible AI, Arabic, and the rest of the surface

**Responsible-AI red team** — 31 curated attacks across jailbreak, prompt-injection, PII
exfiltration, data leakage, SQL injection, excessive agency, toxicity/bias, and **over-refusal**
(a benign question wrongly refused is a failure, not a safe default). Reports ASR per category
and blocks CI on any breach. Optionally extended with the Azure AI Evaluation
`AdversarialSimulator`.

> **Own the gap.** The toxicity/bias case has **no deterministic offline defense** — it needs
> Azure AI Content Safety. It reports **UNVERIFIED**, and it is allowed exactly once in policy.
> Saying *"here is the one thing my offline gate cannot prove, and here is where it does get
> proven"* is far stronger than pretending the suite covers everything.

**Arabic (UAE-critical)** — 5 bilingual policies, 14 question pairs. Two properties are scored
separately: **retrieval parity** (Arabic and English land on the same policy) and **figure
parity** (the numbers agree — a 50,000 AED limit is 50,000 in either language). Where a rule is
genuinely stated in two policies, `alt_docs` names the equally-valid door rather than editing
the corpus to hide the ambiguity. Cross-lingual embeddings via `text-embedding-3-large`.

**Prompt Shields, Document Intelligence, Language** — all three Azure services are **now
provisioned in the Bicep** (they weren't, which meant `PROMPT_SHIELDS_ENABLED` could never take
effect in a deployed environment — the code silently stayed on its regex fallback). Endpoints,
Key Vault secrets, and the feature flags are wired into the Container App. The deterministic
fallbacks remain, so flipping a flag off degrades a capability rather than breaking the agent.

**Agentic retrieval benchmark** — Azure AI Search agentic retrieval (query decomposition →
parallel sub-queries → merge) vs the baseline hybrid retriever, scored on recall/precision/
MRR/nDCG. The gate requires a **≥0.10 recall lift** to justify the extra cost.

**MCP server** — the same nine governed tools exposed over Model Context Protocol, so an
MCP-capable host can ask the same questions. Write/action tools stay approval-gated behind
`MCP_ENABLE_ACTIONS`. It's a breadth signal: one governed tool layer, two protocols.

**Document Intelligence** — OCR on settlement/invoice PDFs, reconciled against `fact_payments`
→ `MATCH` / `AMOUNT_MISMATCH` / `MISSING_IN_LEDGER` / `MISSING_IN_DOC`. *This is what the
retained payments star is for.*

**Streaming HITL UI + Teams** — SSE token streaming; sensitive actions **pause for human
approval** (`/approve/{id}`). Recommendations and alerts publish to Teams as Adaptive Cards.

> **Own this one too.** The streaming router shipped a release ago but was **never mounted** in
> `main.py` — `/ui`, `/ask/stream` and `/approve/{id}` 404'd in every deployment while the
> README described them as working. It's mounted now, and the SSE generator had no error
> handling, so an LLM 429 would have dropped the connection mid-stream instead of emitting an
> error frame. If asked "what did you get wrong?", this is a good, honest answer.

---

## Part 5 — New Q&A

**Q. Which model did you choose and why?**
Two tiers behind a router: GPT-4o for synthesis, GPT-4o-mini for tool-selection and lookups.
The agent's accuracy comes from grounding, not model recall, so I weighted tool-calling
reliability (25%) and groundedness (20%) over raw capability, and scored candidates on one
golden set by swapping only the deployment. The router is off by default — it's an
optimization, not a correctness dependency.

**Q. How do you deploy safely?**
Build once, promote by digest — prod ships the exact artifact test approved. OIDC federation,
so no stored secret, on a least-privilege custom role. Blue-green: green deploys at 0%, takes a
canary slice, soaks; if the smoke test fails, traffic never leaves blue and green is killed.

**Q. What did you do for responsible AI?**
31 adversarial attacks across eight categories including over-refusal, each declaring the
defense layer that must stop it, verified per-layer, blocking CI on any breach. And I'll tell
you the gap: toxicity/bias has no offline defense, so it reports UNVERIFIED and gets proven in
the nightly live gate, not the PR gate.

**Q. How do you handle Arabic?**
Parallel AR/EN policy corpus with two separately-scored properties: do both languages retrieve
the same policy, and do the figures agree. Mistranslate a number and the gate fails — I proved
that by mistranslating one.

**Q. What's the hardest bug you found in this project?**
Not a bug — a class of bug. Three of my CI gates were structurally incapable of failing: one
ran zero tests, one scored a dead agent, one compared two constants. All three were green. And
because nothing was checking, `"ignore all previous instructions"` was walking straight through
the input guardrail. *(Then tell the story in Part 1. This is your best answer.)*

**Q. What would you do next?**
Build the FX conformer so the REST lineage is real end-to-end. Replace the file-based Debezium
landing with an actual Kafka consumer. Drive `redteam_unverified_allowed` to 0 by exercising
Content Safety in CI. Expand the golden set beyond 30 questions.

---

## Part 6 — One-page revision delta

| Topic | Hook |
|---|---|
| **Best story** | Three CI gates could not fail. A green check that can't go red buys false confidence. |
| The bug it hid | `"ignore all previous instructions"` — the canonical injection — was not blocked. |
| Model choice | GPT-4o + GPT-4o-mini behind a router. Grounding beats model IQ, so tool-calling reliability was weighted highest. |
| Delivery | Build once, promote by digest. OIDC, no stored secret, least-privilege role. |
| Safety of deploy | Blue-green + canary soak + auto-rollback. Failure keeps traffic on blue. |
| Two bars | Offline PR gate allows 1 UNVERIFIED; nightly live gate allows 0. |
| Responsible AI | 31 attacks, 8 categories, per-layer verification, over-refusal counts as a failure. |
| Arabic | Retrieval parity + figure parity over a real parallel corpus. |
| Honest gaps | No FX conformer. No Kafka. Toxicity/bias unprovable offline. |
| Closing line | *"I'd rather show you the gate I proved could fail than the dashboard that's always green."* |
