"""
Silver parsing / casting / standardisation.

Covers the spec's Silver steps 1-2: parse raw payloads into proper columns,
cast data types, rename to conformed names, standardise values across sources.

The payments file and the customer CDC feed use different source names; here we
map both to the conformed schema. Pure functions, so they're easy to unit-test.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation

# Source column -> conformed column. Different sources name the same thing
# differently (Oracle CUST_ID, SQL Server CustomerId, Salesforce Id, ...).
CUSTOMER_FIELD_MAP = {
    "id": "customer_id",
    "cust_id": "customer_id",
    "customerid": "customer_id",
    "customer_id": "customer_id",
    "name": "customer_name",
    "full_name": "customer_name",
    "customer_name": "customer_name",
    "email": "email",
    "phone": "phone_number",
    "phone_number": "phone_number",
    "country": "nationality",
    "nationality": "nationality",
    "status": "status",
}


def to_decimal(value):
    """Parse a money string/number into Decimal; None on failure."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def to_date(value):
    """Normalise a date/timestamp string to ISO date 'YYYY-MM-DD'; None on fail.

    Accepts 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS', and trailing-Z ISO strings.
    """
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        return None


def to_bool(value):
    """Standardise a flag to a real bool; None if unrecognised."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("true", "t", "1", "yes", "y"):
        return True
    if text in ("false", "f", "0", "no", "n"):
        return False
    return None


def parse_payment(bronze_row):
    """Conform one Bronze payment row to the Silver payment schema."""
    return {
        "payment_id": (bronze_row.get("payment_id") or "").strip() or None,
        "customer_id": (bronze_row.get("customer_id") or "").strip() or None,
        "account_id": (bronze_row.get("account_id") or "").strip() or None,
        "amount": to_decimal(bronze_row.get("amount")),
        "currency_code": (bronze_row.get("currency") or
                          bronze_row.get("currency_code") or "").strip().upper()
                         or None,
        "payment_type": (bronze_row.get("payment_type") or "").strip().upper()
                        or None,
        "transaction_date": to_date(bronze_row.get("transaction_date")),
        # carry lineage forward
        "source_system": bronze_row.get("source_system"),
        "record_hash": bronze_row.get("record_hash"),
        "ingestion_timestamp": bronze_row.get("ingestion_timestamp"),
        "run_id": bronze_row.get("run_id"),
    }


def standardize_customer(cdc_row):
    """Conform one Bronze customer CDC row to the Silver customer schema.

    Keeps the CDC control columns (operation_type, sequence_number,
    event_timestamp) so the SCD2 engine can apply history.
    """
    conformed = {}
    for source_col, value in cdc_row.items():
        key = source_col.lower()
        if key in CUSTOMER_FIELD_MAP:
            conformed[CUSTOMER_FIELD_MAP[key]] = (
                value.strip() if isinstance(value, str) else value
            )
    conformed["customer_id"] = (
        str(conformed["customer_id"]) if conformed.get("customer_id") is not None
        else None
    )
    conformed["status"] = (
        str(conformed["status"]).upper() if conformed.get("status") else None
    )
    # CDC control columns
    conformed["operation_type"] = cdc_row.get("operation_type")
    conformed["sequence_number"] = cdc_row.get("sequence_number")
    conformed["event_timestamp"] = cdc_row.get("event_timestamp")
    conformed["source_system"] = cdc_row.get("source_system")
    return conformed
