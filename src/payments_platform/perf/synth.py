"""
Deterministic synthetic-data generator for performance / scalability testing.

Generates larger payments datasets and customer CDC streams with a controllable
mix of duplicates, corrupt rows, DQ-invalid rows, and late / out-of-order CDC
events. Deterministic: a fixed ``seed`` produces identical data on every machine
(seeded ``random.Random``; no wall clock), so scalability *correctness* is
unit-testable even though the volumes are large.

Sizes: ``small`` / ``medium`` / ``large`` (or pass an explicit ``n``).
"""
from __future__ import annotations

import random

from payments_platform.silver.dq import ALLOWED_CURRENCIES, ALLOWED_PAYMENT_TYPES

SIZES = {"small": 200, "medium": 2_000, "large": 20_000}

PAYMENT_COLUMNS = ["payment_id", "customer_id", "account_id", "amount",
                   "currency", "payment_type", "transaction_date"]

# customer SCD2 tracked columns (must match orchestration.tasks.CUSTOMER_TRACKED)
CUSTOMER_TRACKED = ["customer_name", "email", "phone_number", "nationality", "status"]

_CURRENCIES = sorted(ALLOWED_CURRENCIES)
_PAYMENT_TYPES = sorted(ALLOWED_PAYMENT_TYPES)
_STATUSES = ["ACTIVE", "DORMANT", "CLOSED"]
_NATIONALITIES = ["AE", "US", "GB", "IN", "SA", "FR"]


def resolve_n(size):
    """Map a size name to a row count (or pass an int through)."""
    if isinstance(size, int):
        return size
    return SIZES[size]


# --------------------------------------------------------------------------- #
# payments
# --------------------------------------------------------------------------- #
def generate_payments(size="small", dup_rate=0.05, invalid_rate=0.05,
                      corrupt_rate=0.02, seed=0):
    """Generate a payments dataset with injected dup/invalid/corrupt records.

    Returns a dict:
      rows           valid + duplicate + invalid dict rows (CSV-parseable shape)
      csv            CSV text (header + rows + malformed corrupt lines)
      columns        PAYMENT_COLUMNS
      n_clean / n_duplicate / n_invalid / n_corrupt   the injected counts

    * duplicate rows repeat an existing payment_id (Silver dedup target),
    * invalid rows are well-formed but break a DQ rule (quarantine target),
    * corrupt rows are malformed CSV lines with the wrong field count
      (Bronze ``_corrupt_record`` target).
    """
    n = resolve_n(size)
    rng = random.Random(seed)

    clean = []
    for i in range(n):
        clean.append({
            "payment_id": "PAY%08d" % (i + 1),
            "customer_id": "CUST%05d" % rng.randint(1, max(1, n // 5)),
            "account_id": "AC%06d" % rng.randint(1, max(1, n // 4)),
            "amount": "%.2f" % round(rng.uniform(1.0, 25_000.0), 2),
            "currency": rng.choice(_CURRENCIES),
            "payment_type": rng.choice(_PAYMENT_TYPES),
            "transaction_date": "2026-06-%02d" % (1 + (i % 28)),
        })

    n_dup = int(n * dup_rate)
    duplicates = [dict(clean[i]) for i in range(n_dup)]   # exact repeats of payment_id

    n_invalid = int(n * invalid_rate)
    invalid = []
    for j in range(n_invalid):
        row = dict(clean[j % max(1, n)])
        row["payment_id"] = "PAYBAD%06d" % (j + 1)
        if j % 2 == 0:
            row["amount"] = "-%.2f" % round(rng.uniform(1.0, 999.0), 2)  # negative
        else:
            row["currency"] = "XXX"                                       # disallowed
        invalid.append(row)

    rows = clean + duplicates + invalid

    n_corrupt = int(n * corrupt_rate)
    corrupt_lines = ["CORRUPT%06d,only,three,fields" % (k + 1) for k in range(n_corrupt)]

    return {
        "rows": rows,
        "csv": _to_csv(rows, PAYMENT_COLUMNS, corrupt_lines),
        "columns": PAYMENT_COLUMNS,
        "n_clean": n,
        "n_duplicate": n_dup,
        "n_invalid": n_invalid,
        "n_corrupt": n_corrupt,
    }


def _to_csv(rows, columns, extra_lines=None):
    out = [",".join(columns)]
    for r in rows:
        out.append(",".join(str(r.get(c, "")) for c in columns))
    out.extend(extra_lines or [])
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- #
# customer CDC stream
# --------------------------------------------------------------------------- #
def generate_cdc_events(size="small", updates_per_customer=2, dup_rate=0.05,
                        seed=0, shuffle=True):
    """Generate a conformed customer CDC stream (apply_scd2 input).

    Each customer gets one INSERT then ``updates_per_customer`` UPDATEs with a
    strictly increasing global ``sequence_number``; a fraction are deletes.
    Duplicates re-emit existing events; the stream is then shuffled so events
    arrive **out of order** (apply_scd2 must still converge to the same state).

    Returns a dict: events (emitted, possibly out-of-order), ordered_events
    (by sequence), n_customers, n_events, n_duplicate, n_late.
    """
    n_customers = max(1, resolve_n(size) // (1 + updates_per_customer))
    rng = random.Random(seed)

    events = []
    seq = 0
    for c in range(n_customers):
        cid = str(100000 + c)
        seq += 1
        events.append(_event(cid, "INSERT", seq, rng, status="ACTIVE"))
        for _ in range(updates_per_customer):
            seq += 1
            events.append(_event(cid, "UPDATE", seq, rng,
                                 status=rng.choice(_STATUSES)))
        if c % 7 == 0:                       # occasional delete
            seq += 1
            events.append(_event(cid, "DELETE", seq, rng, status="CLOSED"))

    ordered = list(events)

    n_dup = int(len(events) * dup_rate)
    duplicates = [dict(events[rng.randrange(len(events))]) for _ in range(n_dup)]
    stream = events + duplicates

    if shuffle:
        rng.shuffle(stream)

    return {
        "events": stream,
        "ordered_events": ordered,
        "n_customers": n_customers,
        "n_events": len(events),
        "n_duplicate": n_dup,
        "n_late": _count_out_of_order(stream),
    }


def _event(customer_id, op, seq, rng, status="ACTIVE"):
    return {
        "customer_id": customer_id,
        "customer_name": "Customer %s" % customer_id,
        "email": "user%s@example.com" % customer_id,
        "phone_number": "+9715%07d" % (int(customer_id) % 10_000_000),
        "nationality": rng.choice(_NATIONALITIES),
        "status": status,
        "operation_type": op,
        "sequence_number": seq,
        "event_timestamp": "2026-06-30T%02d:%02d:%02d" % (
            (seq // 3600) % 24, (seq // 60) % 60, seq % 60),
        "source_system": "cdc.customer",
    }


def _count_out_of_order(stream):
    """Number of events that arrive after a higher sequence has already been
    seen (i.e. late / out-of-order in the emitted stream)."""
    late = 0
    high = float("-inf")
    for e in stream:
        s = e.get("sequence_number", float("-inf"))
        if s < high:
            late += 1
        else:
            high = s
    return late
