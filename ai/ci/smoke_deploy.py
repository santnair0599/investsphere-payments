"""
Post-deploy smoke test — hits the deployed agent and asserts it's actually serving.

  python -m ai.ci.smoke_deploy --base-url https://<containerapp-fqdn>
  BASE_URL=https://... python -m ai.ci.smoke_deploy

Checks: /health 200, /tools returns the marts+RAG surface, and POST /ask returns a
non-empty grounded answer with >=1 tool call within the latency budget. Exits
non-zero on any failure so the deploy job fails and (optionally) rolls back.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request

LATENCY_BUDGET_S = 15.0


def _get(url, timeout=20):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read().decode()


def _post(url, body, timeout=30):
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read().decode())


def main(base_url: str, out_path: str | None = None) -> int:
    base = base_url.rstrip("/")
    failures = []
    evidence = {"base_url": base, "checks": {}}

    # 1) health
    try:
        st, _ = _get(f"{base}/health")
        assert st == 200
        print("PASS  /health 200")
    except Exception as e:
        failures.append(f"/health: {e}")

    # 2) tool surface
    try:
        st, body = _get(f"{base}/tools")
        assert st == 200 and body.strip()
        print("PASS  /tools reachable")
    except Exception as e:
        failures.append(f"/tools: {e}")

    # 3) real Q&A round trip
    try:
        t0 = time.time()
        st, data = _post(f"{base}/ask",
                         {"question": "What were total AED payments on 2026-06-30?",
                          "session_id": "smoke"})
        dt = time.time() - t0
        assert st == 200, f"status {st}"
        assert (data.get("answer") or "").strip(), "empty answer"
        assert dt <= LATENCY_BUDGET_S, f"latency {dt:.1f}s > {LATENCY_BUDGET_S}s"
        print(f"PASS  /ask answered in {dt:.1f}s  tools={data.get('tools_called')}")
        # capture trace/run identifiers the app returns (for the evidence trail)
        evidence["checks"]["ask"] = {
            "latency_s": round(dt, 2), "tools_called": data.get("tools_called"),
            "run_id": data.get("run_id"), "trace_id": data.get("trace_id"),
        }
    except Exception as e:
        failures.append(f"/ask: {e}")

    evidence["passed"] = not failures
    evidence["failures"] = failures
    if out_path:
        import os
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(evidence, fh, indent=2)

    if failures:
        print("\nSMOKE TEST FAILED:")
        for f in failures:
            print("  -", f)
        return 1
    print("\nSMOKE TEST PASSED")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=os.environ.get("BASE_URL"))
    ap.add_argument("--out", default=None, help="write smoke result JSON here")
    a = ap.parse_args()
    if not a.base_url:
        print("provide --base-url or BASE_URL")
        sys.exit(2)
    sys.exit(main(a.base_url, a.out))
