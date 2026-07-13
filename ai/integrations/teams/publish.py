"""
[12] Microsoft Teams publishing.

Pushes agent outputs to a Teams channel as **Adaptive Cards**:
  * publish_recommendation(rec)  -> a BusinessRecommendation (from /recommend)
  * publish_alert(alert)         -> a DQ / freshness / security monitoring alert
  * publish_answer(q, answer, tools) -> an ad-hoc Q&A result

Transport: an Incoming Webhook (TEAMS_WEBHOOK_URL) by default. For richer flows
(mentions, actionable buttons that call back into /approve) use a Graph/Bot app;
this module keeps the webhook path so it works with zero app registration.

Feature flag: TEAMS_ENABLED=true actually POSTs. Unset/false -> returns the card
JSON (so CI and dry-runs exercise card-building without a live channel).

  python -m ai.integrations.teams.publish --demo
"""
from __future__ import annotations

import json
import os
import urllib.request


def _card(title: str, facts: dict, text: str = "", color: str = "0076D7") -> dict:
    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard", "version": "1.4",
                "body": [
                    {"type": "TextBlock", "size": "Large", "weight": "Bolder",
                     "text": title, "color": "Accent"},
                    {"type": "FactSet", "facts": [
                        {"title": str(k), "value": str(v)} for k, v in facts.items()]},
                    *([{"type": "TextBlock", "wrap": True, "text": text}] if text else []),
                ],
            },
        }],
    }


def _send(card: dict) -> dict:
    if os.getenv("TEAMS_ENABLED", "false").lower() != "true":
        return {"dry_run": True, "card": card}
    url = os.environ["TEAMS_WEBHOOK_URL"]
    req = urllib.request.Request(url, data=json.dumps(card).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return {"status": resp.status}


def publish_recommendation(rec: dict) -> dict:
    facts = {
        "Decision": rec.get("decision", "-"),
        "Confidence": rec.get("confidence", "-"),
        "Domain": rec.get("domain", "-"),
        "Grounded on": ", ".join(rec.get("sources", []))[:120] or "-",
    }
    return _send(_card(f"📊 Recommendation: {rec.get('title','Business recommendation')}",
                       facts, rec.get("rationale", ""), color="237804"))


def publish_alert(alert: dict) -> dict:
    facts = {
        "Severity": alert.get("severity", "-"),
        "Rule": alert.get("rule", "-"),
        "Metric": f"{alert.get('metric','-')} = {alert.get('value','-')}",
        "Table": alert.get("table", "-"),
    }
    return _send(_card(f"🚨 {alert.get('title','Monitoring alert')}", facts,
                       alert.get("message", ""), color="A8071A"))


def publish_answer(question: str, answer: str, tools: list[str] | None = None) -> dict:
    facts = {"Question": question[:120], "Tools": ", ".join(tools or []) or "-"}
    return _send(_card("💬 InvestSphere answer", facts, answer))


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true")
    ap.parse_args()
    out = publish_recommendation({
        "title": "Reprice underperforming leases in Downtown portfolio",
        "decision": "REVIEW", "confidence": "0.82", "domain": "real_estate",
        "sources": ["gold_realestate.mart_property_underperformance", "investment_risk_policy"],
        "rationale": "Occupancy < 70% for 3 consecutive months on 4 properties; policy POL-RE-003 "
                     "recommends a pricing review before renewal."})
    print(json.dumps(out, indent=2))
