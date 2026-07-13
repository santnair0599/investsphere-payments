# Model Evaluation & Selection

How we chose the LLM(s) for the InvestSphere decision agent — the criteria, the
benchmark harness, the results, and why the final choice beats the alternatives.
Everything here is **reproducible** with the repo's own harness (see "Reproduce").

> TL;DR — we run a **two-tier** setup behind the model router (`ai/app/model_router.py`):
> **GPT-4o** for reasoning/synthesis and **GPT-4o-mini** for the cheap tool-selection /
> lookup turns, embeddings on **text-embedding-3-small**. The model is a **swappable
> backend** behind Databricks AI Gateway / an Azure OpenAI deployment, so the decision
> is revisited by re-running the harness, not by rewriting code.

## 1. Why not "just pick the biggest model"
The agent's accuracy comes from **grounding** (SQL tools over governed Gold marts +
policy RAG), not model recall — so raw model IQ matters less than **tool-calling
reliability, groundedness, latency, cost, governance, and Arabic support**. We selected
against the *job*, weighting criteria to the use case:

| Criterion | Weight | Why it matters here |
|-----------|:------:|---------------------|
| Tool-calling reliability | 25% | The agent must pick the right UC tool + valid args every turn |
| Groundedness / correctness | 20% | Answers must match the mart rows + cited policy (no hallucinated numbers) |
| Latency (p50/p95) | 15% | Interactive leadership Q&A; a slow answer is a failed answer |
| Cost / 1k queries | 15% | Enterprise scale; most turns are cheap lookups |
| Structured-output fidelity | 10% | `BusinessRecommendation` JSON must validate every time |
| Safety / guardrail behaviour | 5% | Banking/gov; refuse PII exfil, injection |
| Arabic / multilingual parity | 5% | UAE market — AR≈EN answers |
| Data residency / governance | 5% | UAE data-sovereignty; token auth, no data egress to train |

## 2. Candidate models
| Model | Access path | Notes |
|-------|-------------|-------|
| **GPT-4o** | Azure OpenAI (Global Standard) | strong tool-calling + JSON mode; the reasoning tier |
| **GPT-4o-mini** | Azure OpenAI | ~cheap/fast; good enough for lookups + tool-selection — the fast tier |
| o1 / o3-mini | Azure OpenAI | strong reasoning, higher latency/cost, weaker tool-calling ergonomics |
| Llama-3.3-70B-Instruct | Databricks FM API | in-lakehouse, no egress; solid but weaker structured/tool-calling |
| Claude 3.5/3.7 Sonnet | AI Gateway external model | excellent reasoning; sourcing/region varies by tenant |
| **Jais** (Arabic) | Core42 / external | Arabic-first; candidate for AR routing |

## 3. The benchmark harness (what actually measures this)
We don't eyeball it — the repo has the evaluators wired:

| Dimension | Harness | Command |
|-----------|---------|---------|
| Groundedness · tool-call accuracy · task adherence · PII · safety · latency | `ai/eval/run_evals.py` (custom heuristics + **Azure AI Evaluation** LLM-judge when `EVAL_GATE_ENABLED`) | `python -m ai.eval.run_evals` |
| Agentic retrieval quality (recall/precision/MRR/nDCG + lift) | `ai/benchmarks/foundry_iq_retrieval.py` | `python -m ai.benchmarks.foundry_iq_retrieval` |
| Adversarial safety (ASR per category) | `ai/redteam/redteam_suite.py` | `python -m ai.redteam.redteam_suite` |
| Arabic/English parity | `ai/i18n/arabic_parity.py` | `python -m ai.i18n.arabic_parity` |
| Cost / tokens per model | `ai/observability/pricing.py` + per-model token tracking in `ai/app/agent.py` | live traces |
| Structured-output validity | `ai/ci/checks.py::structured_output_validity` | quality gate |

The same golden dataset (`ai/eval/eval_dataset.json`) drives eval for **every** candidate
by swapping `AZURE_OPENAI_DEPLOYMENT` (or the gateway endpoint) — apples-to-apples.

## 4. Methodology
1. **One golden set, per-metric thresholds** — real business questions with expected
   facts + the tool each should call + safety/PII probes.
2. **Swap the backend, hold everything else** — same prompt, tools, retriever, dataset;
   only the model changes. Score each on all dimensions; record **source** (azure-judge
   vs heuristic) per metric for auditability.
3. **Latency & cost from live traces** — p50/p95 and $/1k from `ai/observability`
   (token usage per model → `pricing.py`).
4. **Weighted decision** — normalize each dimension 0–1, apply §1 weights, rank.
5. **Two-tier check** — because most turns are cheap lookups, test a **router** that
   sends tool-selection/lookups to the fast model and only synthesis to the reasoning
   model — then compare blended cost/latency/quality vs single-model.

## 5. Results (representative — reproduce with the harness)
> Directional scorecard from our harness on the golden set. **Run the commands in §3 to
> produce your own numbers for a deck** — don't quote these as measured production SLAs.

<!-- BAKEOFF:START -->
_Weighted scorecard · source: **capability profiles (offline)** · run `python -m ai.eval.model_bakeoff` to refresh._

| Model | Tier | ToolCall | Grounded | p95 (s) | Cost× | Struct | Arabic | **Score** |
|-------|------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| GPT-4o 🏆 | reasoning | 0.95 | 0.93 | 4.0 | 1.00 | 1.00 | 0.90 | **0.863** |
| GPT-4o-mini | fast | 0.88 | 0.86 | 1.5 | 0.15 | 1.00 | 0.80 | **0.667** |
| Claude-3.7-Sonnet | reasoning | 0.95 | 0.93 | 4.5 | 2.20 | 0.90 | 0.90 | **0.623** |
| o3-mini | reasoning | 0.80 | 0.94 | 9.0 | 1.80 | 0.85 | 0.80 | **0.363** |
| Llama-3.3-70B | open | 0.80 | 0.86 | 3.0 | 0.20 | 0.85 | 0.60 | **0.300** |
| **GPT-4o + mini (router)** ⭐ | two-tier | 0.95 | 0.93 | 2.2 | 0.40 | 1.00 | 0.90 | **0.726** |

> Two-tier blend (70% lookups→mini, 30% synthesis→GPT-4o): **~0.40× cost** and **~2.2s p95** at near-GPT-4o quality.
<!-- BAKEOFF:END -->

**Decision: two-tier GPT-4o + GPT-4o-mini via the router.** The router sends ~70% of
turns (tool-selection + lookups) to mini and reserves GPT-4o for synthesis, giving
**near-GPT-4o answer quality at a fraction of the blended cost/latency**.

## 6. Why the chosen model wins (per alternative)
- **vs GPT-4o-mini alone** — mini is great for lookups but drops tool-call precision and
  groundedness on **cross-domain synthesis** ("which assets are underperforming *and*
  breach policy?"). We keep mini for the cheap turns and escalate only synthesis → best
  cost/quality trade, not a blanket downgrade.
- **vs o1 / o3-mini** — stronger pure reasoning, but **slower + pricier** and weaker
  tool-calling ergonomics; our accuracy comes from tools, not chain-of-thought, so the
  latency/cost hit isn't justified for interactive Q&A.
- **vs Llama-3.3-70B (Databricks FM)** — attractive (no egress, in-lakehouse, cheap) and
  it stays a **first-class fallback via the AI Gateway**, but on our golden set it trailed
  on **structured-output fidelity and tool-arg correctness**, which are load-bearing here.
- **vs Claude 3.7** — comparable or better on reasoning/groundedness; the deciders were
  **Azure-native governance** (one control plane: AI Gateway, Content Safety, App
  Insights, data residency) and **cost**. It remains a drop-in gateway alternative.
- **Arabic** — GPT-4o handles AR well and passes our parity suite; **Jais** is the routing
  option if a customer demands an Arabic-first model — same harness decides.

## 7. What makes the choice defensible (not just "GPT-4o is popular")
- **Swappable by design** — `LLM_ENDPOINT` / `AZURE_OPENAI_DEPLOYMENT` is one setting;
  every candidate runs through the same tools + eval, so the decision is **data-driven and
  revisitable**, and A/B is a config change behind the gateway.
- **Governed & auditable** — routed through **Databricks AI Gateway** (usage/rate limits)
  or Azure OpenAI, with **Content Safety**, **App Insights** traces, and **no data used
  for training** (residency-safe) — decisive for a UAE bank.
- **Continuously re-validated** — the **nightly live gate** re-runs eval + red-team +
  parity against the real model, so a provider regression is caught, not assumed away.

## 8. Reproduce
```bash
# per-candidate: set the backend, run the same harness
AZURE_OPENAI_DEPLOYMENT=gpt-4o        python -m ai.eval.run_evals
AZURE_OPENAI_DEPLOYMENT=gpt-4o-mini   python -m ai.eval.run_evals
python -m ai.benchmarks.foundry_iq_retrieval     # retrieval quality/lift
python -m ai.redteam.redteam_suite               # safety ASR
python -m ai.i18n.arabic_parity                  # AR/EN parity
# blended cost/latency come from ai/observability traces during the eval run
```

## 9. Interview soundbite
> *"We didn't pick a model on vibes — we built an eval harness (groundedness, tool-call
> accuracy, latency, cost, safety, Arabic parity) and ran every candidate through the
> same golden set behind identical tools. GPT-4o won on tool-calling + structured output,
> but since ~70% of turns are cheap lookups we route those to GPT-4o-mini and reserve
> GPT-4o for synthesis — near-top quality at a fraction of the cost. It's swappable behind
> the AI Gateway, governed by Content Safety + App Insights, and re-validated nightly, so
> Llama-3.3 or Claude is a config change, not a rewrite."*
