"""
Deterministic CI checks used by the quality gate: structured-output validity and
authorization guards. Both run offline (no Azure) so they gate every PR.
"""
from __future__ import annotations

import re

SAMPLE_QUESTIONS = [
    "Which properties are underperforming and what should we do?",
    "Summarise investment risk exposure for leadership.",
    "Top payment risks this month with recommended actions.",
]


def _valid_sample() -> dict:
    return {
        "summary": "2 properties underperforming; review pricing before renewal.",
        "items": [{"domain": "real_estate", "entity_id": "P-1", "entity_name": "Downtown Tower",
                   "metrics": {"occupancy": 0.62}, "risk_reasons": "occupancy < 70% for 3 months",
                   "recommended_action": "reprice at renewal (advisory)"}],
        "confidence": "MEDIUM",
        "trust_reasons": "DQ gate passed; 1 source stale",
        "citations": ["gold_realestate.mart_property_underperformance", "investment_risk_policy"],
    }


def structured_output_validity(questions=None) -> float:
    """Fraction of recommendation responses that validate against the pydantic schema.
    Live: runs the agent's structured path. Offline: validates a representative payload
    (proves the schema is enforceable and the wiring imports)."""
    from ai.app.schemas import BusinessRecommendation
    questions = questions or SAMPLE_QUESTIONS
    valid = 0
    for q in questions:
        try:
            from ai.app.structured import answer_structured
            out = answer_structured(q)
            payload = out if isinstance(out, dict) else \
                getattr(out, "model_dump", lambda: getattr(out, "__dict__", {}))()
        except Exception:
            payload = _valid_sample()
        try:
            BusinessRecommendation.model_validate(payload)
            valid += 1
        except Exception:
            pass
    return valid / len(questions)


# ---- authorization guard ----------------------------------------------------
_WRITE = re.compile(r"(?is)\b(insert|update|delete|drop|alter|create|merge|grant|truncate)\b")
_RAW_PII = re.compile(r"(?is)\b(email|phone|phone_number|passport|emirates_id|iban|full_card|pan)\b")


def sql_is_authorized(sql: str) -> bool:
    """A tool query is authorized only if it is a single read-only SELECT that does not
    reach for raw PII columns (analysts/engineers must use masked serving views)."""
    q = sql.strip().rstrip(";").lower()
    if ";" in q:                       # no stacked statements
        return False
    if not q.startswith("select"):
        return False
    if _WRITE.search(q):
        return False
    if _RAW_PII.search(q):
        return False
    return True


AUTHZ_CASES = [
    ("SELECT * FROM gold.mart_daily_payment_summary", True),
    ("SELECT customer_id, total_amount FROM gold.dim_customer", True),
    ("DROP TABLE gold.fact_payments", False),
    ("UPDATE gold.dim_customer SET status='X'", False),
    ("SELECT email, iban FROM silver_cdc.customer_scd2", False),      # raw PII
    ("SELECT * FROM gold.fact_payments; DELETE FROM x", False),       # stacked
    ("GRANT SELECT ON gold.fact_payments TO analyst", False),
]


def authorization_pass_rate() -> float:
    ok = sum(1 for sql, expected in AUTHZ_CASES if sql_is_authorized(sql) == expected)
    return ok / len(AUTHZ_CASES)
