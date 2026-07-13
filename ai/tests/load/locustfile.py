"""
[8a] Load test for the Enterprise Decision Agent API (ai/app/main.py).

Drives realistic business questions against /ask and /recommend and asserts SLOs
(p95 latency, error rate). Run against a deployed Container App or a local uvicorn.

  # start the API:  uvicorn ai.app.main:app --port 8080
  locust -f ai/tests/load/locustfile.py --host http://localhost:8080 \
         --users 50 --spawn-rate 5 --run-time 3m --headless

SLO gate (checked in on_stop / CI): p95 < 4000ms, failure ratio < 1%.
"""
from __future__ import annotations

import random
from locust import HttpUser, task, between, events

QUESTIONS = [
    "What were total USD payments on 2026-06-30?",
    "Who are the top 5 customers by total payment amount?",
    "Does a 60,000 AED single payment breach the payment-limit policy?",
    "What was customer C001's status on 2026-06-30?",
    "Which currency had the highest total value last month?",
    "Summarise payment volume by type for the latest day.",
]

# SLOs
P95_MS = 4000
MAX_FAIL_RATIO = 0.01


class AgentUser(HttpUser):
    wait_time = between(1, 4)

    @task(4)
    def ask(self):
        q = random.choice(QUESTIONS)
        with self.client.post("/ask", json={"question": q, "session_id": "load"},
                              catch_response=True, name="POST /ask") as r:
            if r.status_code != 200:
                r.failure(f"status {r.status_code}")
            elif not (r.json() or {}).get("answer"):
                r.failure("empty answer")

    @task(1)
    def recommend(self):
        self.client.post("/recommend", json={"question": random.choice(QUESTIONS)},
                         name="POST /recommend")

    @task(1)
    def health(self):
        self.client.get("/health", name="GET /health")


@events.quitting.add_listener
def _assert_slos(environment, **_):
    stats = environment.stats.total
    p95 = stats.get_response_time_percentile(0.95)
    fail_ratio = stats.fail_ratio
    ok = True
    if p95 and p95 > P95_MS:
        print(f"SLO FAIL: p95 {p95:.0f}ms > {P95_MS}ms"); ok = False
    if fail_ratio > MAX_FAIL_RATIO:
        print(f"SLO FAIL: failure ratio {fail_ratio:.2%} > {MAX_FAIL_RATIO:.2%}"); ok = False
    environment.process_exit_code = 0 if ok else 1
    print("SLO PASS" if ok else "SLO GATE FAILED")
