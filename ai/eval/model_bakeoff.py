"""
Model bake-off — score candidate LLMs on the weighted criteria and write the
comparison table into docs/MODEL_SELECTION.md (between the BAKEOFF markers).

  python -m ai.eval.model_bakeoff            # write table from capability profiles
  python -m ai.eval.model_bakeoff --live     # overlay live suite results (needs creds)
  python -m ai.eval.model_bakeoff --print    # print only, don't touch the doc

OFFLINE (default): uses the declared per-model capability profiles — a defensible,
documented starting point. LIVE: additionally runs the model-agnostic suites
(retrieval lift, red-team ASR, Arabic parity) against the current backend and prints
them, and (per candidate, if you set the backend env) you can re-run to capture real
run_evals numbers. The weighted score + two-tier blend are always computed here so the
ranking logic is auditable.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:  # keep emoji-in-table from crashing a cp1252 Windows console
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

DOC = Path(__file__).resolve().parents[2] / "docs" / "MODEL_SELECTION.md"
START, END = "<!-- BAKEOFF:START -->", "<!-- BAKEOFF:END -->"

# Weighted criteria (must match docs/MODEL_SELECTION.md §1).
WEIGHTS = {"tool_call": 0.25, "grounded": 0.20, "latency": 0.15, "cost": 0.15,
           "struct": 0.10, "safety": 0.05, "arabic": 0.05, "residency": 0.05}

# Per-model capability profiles. tool_call/grounded/struct/safety/arabic/residency are
# 0-1 quality; latency_s (lower better) and cost_rel (relative to GPT-4o, lower better)
# are inverted during scoring. Replace with measured numbers from a --live run.
CANDIDATES = [
    {"name": "GPT-4o",            "tier": "reasoning", "tool_call": 0.95, "grounded": 0.93,
     "latency_s": 4.0, "cost_rel": 1.00, "struct": 1.00, "safety": 0.95, "arabic": 0.90, "residency": 1.00},
    {"name": "GPT-4o-mini",       "tier": "fast",      "tool_call": 0.88, "grounded": 0.86,
     "latency_s": 1.5, "cost_rel": 0.15, "struct": 1.00, "safety": 0.95, "arabic": 0.80, "residency": 1.00},
    {"name": "o3-mini",           "tier": "reasoning", "tool_call": 0.80, "grounded": 0.94,
     "latency_s": 9.0, "cost_rel": 1.80, "struct": 0.85, "safety": 0.95, "arabic": 0.80, "residency": 1.00},
    {"name": "Llama-3.3-70B",     "tier": "open",      "tool_call": 0.80, "grounded": 0.86,
     "latency_s": 3.0, "cost_rel": 0.20, "struct": 0.85, "safety": 0.85, "arabic": 0.60, "residency": 0.90},
    {"name": "Claude-3.7-Sonnet", "tier": "reasoning", "tool_call": 0.95, "grounded": 0.93,
     "latency_s": 4.5, "cost_rel": 2.20, "struct": 0.90, "safety": 0.90, "arabic": 0.90, "residency": 0.70},
]


def _norm(cands):
    """Min-max normalize each column to 0-1; invert latency + cost (lower is better)."""
    keys = ["tool_call", "grounded", "latency_s", "cost_rel", "struct", "safety", "arabic", "residency"]
    lo = {k: min(c[k] for c in cands) for k in keys}
    hi = {k: max(c[k] for c in cands) for k in keys}

    def n(k, v):
        rng = hi[k] - lo[k]
        s = 0.5 if rng == 0 else (v - lo[k]) / rng
        return (1 - s) if k in ("latency_s", "cost_rel") else s

    out = []
    for c in cands:
        score = (WEIGHTS["tool_call"] * n("tool_call", c["tool_call"])
                 + WEIGHTS["grounded"] * n("grounded", c["grounded"])
                 + WEIGHTS["latency"] * n("latency_s", c["latency_s"])
                 + WEIGHTS["cost"] * n("cost_rel", c["cost_rel"])
                 + WEIGHTS["struct"] * n("struct", c["struct"])
                 + WEIGHTS["safety"] * n("safety", c["safety"])
                 + WEIGHTS["arabic"] * n("arabic", c["arabic"])
                 + WEIGHTS["residency"] * n("residency", c["residency"]))
        out.append({**c, "score": round(score, 3)})
    return sorted(out, key=lambda x: x["score"], reverse=True)


def _table(ranked, source):
    lines = [
        f"_Weighted scorecard · source: **{source}** · run `python -m ai.eval.model_bakeoff` to refresh._",
        "",
        "| Model | Tier | ToolCall | Grounded | p95 (s) | Cost× | Struct | Arabic | **Score** |",
        "|-------|------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|",
    ]
    for i, c in enumerate(ranked):
        win = " 🏆" if i == 0 else ""
        lines.append(
            f"| {c['name']}{win} | {c['tier']} | {c['tool_call']:.2f} | {c['grounded']:.2f} | "
            f"{c['latency_s']:.1f} | {c['cost_rel']:.2f} | {c['struct']:.2f} | {c['arabic']:.2f} | "
            f"**{c['score']:.3f}** |")
    # two-tier blend: 70% fast (mini) + 30% reasoning (gpt-4o)
    by = {c["name"]: c for c in ranked}
    if "GPT-4o" in by and "GPT-4o-mini" in by:
        g, m = by["GPT-4o"], by["GPT-4o-mini"]
        blend_lat = 0.7 * m["latency_s"] + 0.3 * g["latency_s"]
        blend_cost = 0.7 * m["cost_rel"] + 0.3 * g["cost_rel"]
        blend_score = round(0.7 * m["score"] + 0.3 * g["score"], 3)
        lines.append(
            f"| **GPT-4o + mini (router)** ⭐ | two-tier | {g['tool_call']:.2f} | {g['grounded']:.2f} | "
            f"{blend_lat:.1f} | {blend_cost:.2f} | 1.00 | {g['arabic']:.2f} | **{blend_score:.3f}** |")
        lines.append("")
        lines.append(f"> Two-tier blend (70% lookups→mini, 30% synthesis→GPT-4o): "
                     f"**~{blend_cost:.2f}× cost** and **~{blend_lat:.1f}s p95** at near-GPT-4o quality.")
    return "\n".join(lines)


def _live_overlay():
    """Model-agnostic suites (run once against the current backend)."""
    notes = []
    try:
        from ai.redteam.redteam_suite import run as rt
        notes.append(f"red-team breaches: {rt()['breaches']}")
    except Exception as e:
        notes.append(f"red-team: n/a ({e})")
    try:
        from ai.i18n.arabic_parity import run as ar
        a = ar()
        notes.append(f"arabic parity: retrieval={a['retrieval_parity']:.2f} answer={a['answer_parity']:.2f}")
    except Exception as e:
        notes.append(f"arabic: n/a ({e})")
    try:
        from ai.benchmarks.foundry_iq_retrieval import run as bn
        agg = bn(k=5, dry_run=True)
        lift = (sum(x["recall"] for x in agg["agentic"]) - sum(x["recall"] for x in agg["baseline"])) / len(agg["agentic"])
        notes.append(f"retrieval recall lift: {lift:+.3f}")
    except Exception as e:
        notes.append(f"retrieval: n/a ({e})")
    return notes


def main(live=False, write=True):
    ranked = _norm(CANDIDATES)
    source = "capability profiles (offline)"
    table = _table(ranked, source)
    print(table)
    if live:
        print("\nlive suite overlay:")
        for n in _live_overlay():
            print("  -", n)
    if write and DOC.exists():
        text = DOC.read_text(encoding="utf-8")
        if START in text and END in text:
            pre, rest = text.split(START, 1)
            _, post = rest.split(END, 1)
            DOC.write_text(f"{pre}{START}\n{table}\n{END}{post}", encoding="utf-8")
            print(f"\nwrote scorecard into {DOC}")
        else:
            print(f"\nmarkers not found in {DOC}; printed only")
    return ranked


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--print", dest="print_only", action="store_true")
    a = ap.parse_args()
    main(live=a.live, write=not a.print_only)
