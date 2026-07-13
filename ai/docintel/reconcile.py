"""
[7] Document Intelligence reconciliation use case.

Vendor settlement statements / invoices arrive as PDFs. Azure AI **Document
Intelligence** (prebuilt-invoice / layout) extracts the structured fields; we then
**reconcile** each extracted line against the governed Gold payments fact
(`gold.fact_payments`) and flag MATCH / AMOUNT_MISMATCH / MISSING_IN_LEDGER /
MISSING_IN_DOC. The result is both an agent tool and a batch report.

  python -m ai.docintel.reconcile --pdf ai/docintel/samples/settlement.pdf
  python -m ai.docintel.reconcile --dry-run     # uses samples/settlement_extracted.json

Feature flag: DOC_INTEL_ENABLED=true calls the real service (AZURE_DOC_INTEL_*).
Unset/false or --dry-run → load a pre-extracted JSON so the reconciliation logic is
testable without Azure or a real PDF.
"""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, asdict
from decimal import Decimal
from pathlib import Path

from ai.tools.databricks_client import catalog as _catalog

SAMPLES = Path(__file__).parent / "samples"
AMOUNT_TOLERANCE = Decimal("0.01")


@dataclass
class DocLine:
    payment_id: str
    amount: Decimal
    currency: str
    transaction_date: str


# ---- 1) extraction ----------------------------------------------------------
def extract(pdf_path: str | None) -> list[DocLine]:
    if os.getenv("DOC_INTEL_ENABLED", "false").lower() == "true" and pdf_path:
        return _extract_azure(pdf_path)
    # fallback: pre-extracted sample
    raw = json.loads((SAMPLES / "settlement_extracted.json").read_text(encoding="utf-8"))
    return [DocLine(r["payment_id"], Decimal(str(r["amount"])), r["currency"],
                    r["transaction_date"]) for r in raw]


def _extract_azure(pdf_path: str) -> list[DocLine]:
    from azure.core.credentials import AzureKeyCredential
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    client = DocumentIntelligenceClient(
        os.environ["AZURE_DOC_INTEL_ENDPOINT"],
        AzureKeyCredential(os.environ["AZURE_DOC_INTEL_KEY"]))
    with open(pdf_path, "rb") as fh:
        poller = client.begin_analyze_document("prebuilt-invoice", body=fh)
    result = poller.result()
    lines: list[DocLine] = []
    for doc in result.documents or []:
        for item in (doc.fields.get("Items", {}).get("valueArray", []) or []):
            f = item.get("valueObject", {})
            lines.append(DocLine(
                payment_id=(f.get("ProductCode", {}) or {}).get("valueString", ""),
                amount=Decimal(str((f.get("Amount", {}) or {}).get("valueCurrency", {}).get("amount", 0))),
                currency=(f.get("Amount", {}) or {}).get("valueCurrency", {}).get("currencyCode", ""),
                transaction_date=(f.get("Date", {}) or {}).get("valueDate", "")))
    return lines


# ---- 2) ledger lookup (governed Gold) ---------------------------------------
def ledger_lookup(payment_ids: list[str]) -> dict[str, dict]:
    try:
        from ai.app.tools_router import query_gold          # reuses the read-only warehouse tool
        ids = ",".join(f"'{p}'" for p in payment_ids)
        rows = query_gold(
            f"SELECT payment_id, amount, currency_code, transaction_date "
            f"FROM {_catalog()}.gold.fact_payments "
            f"WHERE payment_id IN ({ids})")
        return {r["payment_id"]: r for r in rows}
    except Exception:
        raw = json.loads((SAMPLES / "ledger_fact_payments.json").read_text(encoding="utf-8"))
        return {r["payment_id"]: r for r in raw if r["payment_id"] in payment_ids}


# ---- 3) reconcile -----------------------------------------------------------
def reconcile(pdf_path: str | None = None) -> dict:
    doc_lines = extract(pdf_path)
    ledger = ledger_lookup([d.payment_id for d in doc_lines])
    results, mism, matched = [], 0, 0
    seen = set()
    for d in doc_lines:
        seen.add(d.payment_id)
        led = ledger.get(d.payment_id)
        if not led:
            status = "MISSING_IN_LEDGER"
        elif abs(Decimal(str(led["amount"])) - d.amount) > AMOUNT_TOLERANCE \
                or led["currency_code"] != d.currency:
            status = "AMOUNT_MISMATCH"
        else:
            status = "MATCH"
        results.append({"payment_id": d.payment_id, "doc_amount": str(d.amount),
                        "ledger_amount": str(led["amount"]) if led else None,
                        "currency": d.currency, "status": status})
        matched += status == "MATCH"
        mism += status != "MATCH"
    # payments in the ledger but absent from the doc
    for pid, led in ledger.items():
        if pid not in seen:
            results.append({"payment_id": pid, "doc_amount": None,
                            "ledger_amount": str(led["amount"]), "status": "MISSING_IN_DOC"})
            mism += 1
    report = {"total_doc_lines": len(doc_lines), "matched": matched,
              "exceptions": mism, "lines": results}
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    rep = reconcile(None if a.dry_run else a.pdf)
    print(json.dumps(rep, indent=2, default=str))
    print(f"\n{rep['matched']}/{rep['total_doc_lines']} matched - {rep['exceptions']} exceptions")
