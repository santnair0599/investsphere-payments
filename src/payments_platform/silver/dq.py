"""
Silver data-quality checks.

Covers the spec's Silver step 3: reject/separate bad data before it becomes
trusted. Each rule is a small function ``(row) -> None | reason``; a rule set is
just a list of (rule_name, severity, fn). ``evaluate`` runs them and returns the
failures per row, which :mod:`quarantine` uses to split valid vs invalid.

On Databricks the equivalent is Lakeflow expectations
(``@dp.expect`` / ``expect_or_drop`` / ``expect_or_fail``) plus a quarantine
pattern — same intent, same severities (warn / drop / fail).
"""
from __future__ import annotations

FAIL = "FAIL"            # unrecoverable -> stop the pipeline
QUARANTINE = "QUARANTINE"  # route the row out, keep processing
WARN = "WARN"            # count it, let the row through

ALLOWED_CURRENCIES = {"AED", "USD", "EUR", "GBP", "INR", "SAR"}
ALLOWED_PAYMENT_TYPES = {"CREDIT", "DEBIT", "TRANSFER", "REFUND"}


# ---- payment rules ----------------------------------------------------------
def _payment_id_present(row):
    if not row.get("payment_id"):
        return "missing payment_id"


def _customer_id_present(row):
    if not row.get("customer_id"):
        return "missing customer_id"


def _amount_not_null(row):
    if row.get("amount") is None:
        return "amount not parseable / null"


def _amount_not_negative(row):
    amount = row.get("amount")
    if amount is not None and amount < 0:
        return "amount is negative: " + str(amount)


def _currency_allowed(row):
    cur = row.get("currency_code")
    if cur is not None and cur not in ALLOWED_CURRENCIES:
        return "currency not allowed: " + str(cur)


def _type_allowed(row):
    ptype = row.get("payment_type")
    if ptype is not None and ptype not in ALLOWED_PAYMENT_TYPES:
        return "payment_type not allowed: " + str(ptype)


def _transaction_date_valid(row):
    if not row.get("transaction_date"):
        return "transaction_date missing / invalid format"


# (rule_name, severity, fn). payment_id missing is FAIL-worthy (unauditable);
# everything else quarantines the row.
PAYMENT_RULES = [
    ("payment_id_present", FAIL, _payment_id_present),
    ("customer_id_present", QUARANTINE, _customer_id_present),
    ("amount_not_null", QUARANTINE, _amount_not_null),
    ("amount_not_negative", QUARANTINE, _amount_not_negative),
    ("currency_allowed", QUARANTINE, _currency_allowed),
    ("payment_type_allowed", QUARANTINE, _type_allowed),
    ("transaction_date_valid", QUARANTINE, _transaction_date_valid),
]


def evaluate(row, rules):
    """Run ``rules`` against one row.

    Returns a list of failures: ``[{"rule": name, "severity": sev,
    "reason": text}, ...]`` (empty if the row passes all rules).
    """
    failures = []
    for rule_name, severity, fn in rules:
        reason = fn(row)
        if reason is not None:
            failures.append({"rule": rule_name, "severity": severity,
                             "reason": reason})
    return failures


class MandatoryFieldError(ValueError):
    """Raised when a FAIL-severity rule breaks -> the Silver job must stop."""
