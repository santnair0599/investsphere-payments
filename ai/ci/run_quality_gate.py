"""
Quality gate — aggregates every AI check and enforces ai/ci/quality_gates.yaml.

  python -m ai.ci.run_quality_gate

Runs: unit/contract tests · chaos/failure tests · red-team regression · Arabic
parity · retrieval benchmark · structured-output validity · authorization tests.
Prints a scorecard and EXITS NON-ZERO on any breach, so it can be a required PR
check that blocks merge (and a `needs:` dependency for the deploy-to-test job).
"""
from __future__ import annotations

import json
import operator as op
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GATES = Path(__file__).parent / "quality_gates.yaml"

DEFAULTS = {
    "unit_tests_required": True, "redteam_breaches_allowed": 0,
    "arabic_retrieval_pass_rate": 1.0, "arabic_answer_parity": 1.0,
    "minimum_retrieval_recall_lift": 0.10, "structured_output_validity": 1.0,
    "critical_chaos_test_failures_allowed": 0, "authorization_pass_rate": 1.0,
}


def load_policy() -> dict:
    try:
        import yaml
        return {**DEFAULTS, **(yaml.safe_load(GATES.read_text(encoding="utf-8")) or {}).get("quality_gates", {})}
    except Exception:
        return dict(DEFAULTS)


def pytest_failures(path: str) -> int:
    """Return failed-test count for a path (0 = pass / no tests, 999 = collection error)."""
    if not (ROOT / path.split("::")[0]).exists():
        return 0
    r = subprocess.run([sys.executable, "-m", "pytest", path, "-q", "-p", "no:cacheprovider",
                        "--ignore=.dbt-venv", "--ignore=.venv"],
                       cwd=ROOT, capture_output=True, text=True)
    out = r.stdout + r.stderr
    m = re.search(r"(\d+) failed", out)
    if m:
        return int(m.group(1))
    if r.returncode in (0, 5):          # 0 = passed, 5 = no tests collected
        return 0
    return 999                          # collection / import error


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def main() -> int:
    p = load_policy()
    rows = []  # (metric, value, threshold, passed)

    def gate(metric, value, cmp, thr):
        passed = bool(cmp(value, thr))
        rows.append((metric, value, thr, passed))

    # --- unit + contract tests ------------------------------------------------
    if p["unit_tests_required"]:
        unit_path = "tests" if (ROOT / "tests").exists() else "src"
        gate("unit_test_failures", pytest_failures(unit_path), op.le, 0)

    # --- chaos / failure tests ------------------------------------------------
    gate("critical_chaos_failures", pytest_failures("ai/tests/failure/chaos.py"),
         op.le, p["critical_chaos_test_failures_allowed"])

    # --- red-team regression --------------------------------------------------
    from ai.redteam.redteam_suite import run as redteam_run
    gate("redteam_breaches", redteam_run()["breaches"], op.le, p["redteam_breaches_allowed"])

    # --- Arabic parity --------------------------------------------------------
    from ai.i18n.arabic_parity import run as arabic_run
    ar = arabic_run()
    gate("arabic_retrieval_pass_rate", ar["retrieval_parity"], op.ge, p["arabic_retrieval_pass_rate"])
    gate("arabic_answer_parity", ar["answer_parity"], op.ge, p["arabic_answer_parity"])

    # --- retrieval benchmark lift --------------------------------------------
    from ai.benchmarks.foundry_iq_retrieval import run as bench_run
    agg = bench_run(k=5, dry_run=True)
    lift = round(_mean([x["recall"] for x in agg["agentic"]]) -
                 _mean([x["recall"] for x in agg["baseline"]]), 3)
    gate("retrieval_recall_lift", lift, op.ge, p["minimum_retrieval_recall_lift"])

    # --- structured output + authorization -----------------------------------
    from ai.ci.checks import structured_output_validity, authorization_pass_rate
    gate("structured_output_validity", structured_output_validity(), op.ge, p["structured_output_validity"])
    gate("authorization_pass_rate", authorization_pass_rate(), op.ge, p["authorization_pass_rate"])

    # --- scorecard ------------------------------------------------------------
    print("\n" + "=" * 68)
    print(" AI QUALITY GATE SCORECARD")
    print("=" * 68)
    all_pass = True
    for metric, value, thr, passed in rows:
        all_pass &= passed
        print(f"  {'PASS' if passed else 'FAIL'}  {metric:34s} {str(value):>7}   (req {thr})")
    print("=" * 68)
    print(" RESULT:", "PASS -> deploy to test" if all_pass else "FAIL -> block merge")

    # evidence report (uploaded as a CI artifact even on failure)
    report = {
        "result": "pass" if all_pass else "fail",
        "metrics": [{"metric": m, "value": v, "threshold": t, "passed": bool(pz)}
                    for (m, v, t, pz) in rows],
    }
    out = Path(os.environ.get("GATE_REPORT", ROOT / "ai" / "ci" / "_gate_report.json"))
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f" evidence: {out}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
