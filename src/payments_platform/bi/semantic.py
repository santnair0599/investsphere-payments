"""
The declarative BI semantic model.

Everything BI-facing is defined here: the analyst-safe **datasets** (each backed
by a Gold mart / masked view / non-PII fact), the business **measures** (KPIs),
the **dimensions** Power BI slices by, and the **row-level-security** mapping that
lines up with the governance region row filter.

PII classification and the region columns are sourced from
:mod:`governance.policy`, so the BI layer can never drift from the security model
the governance tests enforce.
"""
from __future__ import annotations

from payments_platform.governance import policy as P

# Raw PII column names (single source of truth = the governance classification).
RAW_PII_COLUMNS = set(P.PII_CLASS)

# Columns governance applies a region row filter on (analysts are filtered).
REGION_COLUMNS = {t["region_col"] for t in P.TABLES if t["region_col"]}

# Schemas Power BI / the analysts group may read from (granted USE SCHEMA).
ANALYST_SCHEMAS = {"gold_marts", "gold_masked"}

# Where the BI serving views are created (analyst-granted schema).
BI_SCHEMA = "gold_marts"


# ---------------------------------------------------------------------------
# Datasets — the BI serving views and the Gold object each is built from.
# `source` is the upstream object; `source_tier` records why it is analyst-safe:
#   mart        -> a pre-aggregated Gold mart (no PII)
#   masked_view -> a governance masked view (PII display-masked / dropped)
#   fact        -> the non-PII payment fact
# `masked_columns` are shown masked (never raw); `rls_column` drives RLS.
# ---------------------------------------------------------------------------
DATASETS = [
    {"name": "payments_daily",
     "view": "v_payments_daily_bi",
     "source": "gold_marts.daily_payment_summary", "source_tier": "mart",
     "columns": ["transaction_date", "currency_code", "payment_type",
                 "customer_country", "payment_count", "total_amount", "avg_amount"],
     "masked_columns": [], "audience": ["analysts"],
     "rls_column": "customer_country",
     "description": "Daily payment revenue by currency / type / country."},

    {"name": "payments_fact",
     "view": "v_payments_fact_bi",
     "source": "gold.fact_payments", "source_tier": "fact",
     "columns": ["payment_id", "customer_id", "account_id", "amount",
                 "currency_code", "payment_type", "transaction_date",
                 "customer_country", "source_system"],
     "masked_columns": [], "audience": ["analysts"],
     "rls_column": "customer_country",
     "description": "Payment-grain fact (no PII), enriched with customer country."},

    {"name": "customer_dim",
     "view": "v_customer_bi",
     "source": "gold_masked.v_customer_masked_for_analytics",
     "source_tier": "masked_view",
     "columns": ["customer_id", "nationality", "status", "source_system",
                 "customer_name", "email", "phone_number"],
     "masked_columns": ["customer_name", "email", "phone_number"],
     "audience": ["analysts"], "rls_column": "nationality",
     "description": "Customer dimension with PII display-masked (analyst-safe)."},
]

DATASET_BY_NAME = {d["name"]: d for d in DATASETS}


# ---------------------------------------------------------------------------
# Measures — business KPIs. `expr` is the SQL/DAX-style aggregation; `column`
# is the dataset column it reads (validated to exist). `format` is a hint.
# ---------------------------------------------------------------------------
MEASURES = [
    {"name": "total_payment_amount", "dataset": "payments_daily",
     "column": "total_amount", "agg": "SUM", "format": "currency",
     "description": "Total payment value."},
    {"name": "payment_count", "dataset": "payments_daily",
     "column": "payment_count", "agg": "SUM", "format": "integer",
     "description": "Number of payments."},
    {"name": "avg_payment_amount", "dataset": "payments_fact",
     "column": "amount", "agg": "AVG", "format": "currency",
     "description": "Average payment value."},
    {"name": "active_customers", "dataset": "payments_fact",
     "column": "customer_id", "agg": "DISTINCT_COUNT", "format": "integer",
     "description": "Distinct paying customers."},
    {"name": "distinct_currencies", "dataset": "payments_daily",
     "column": "currency_code", "agg": "DISTINCT_COUNT", "format": "integer",
     "description": "Currencies transacted."},
]

MEASURE_BY_NAME = {m["name"]: m for m in MEASURES}


# ---------------------------------------------------------------------------
# Dimensions — what Power BI slices/filters by.
# ---------------------------------------------------------------------------
DIMENSIONS = [
    {"name": "transaction_date", "dataset": "payments_daily",
     "column": "transaction_date", "type": "date"},
    {"name": "currency", "dataset": "payments_daily",
     "column": "currency_code", "type": "text"},
    {"name": "payment_type", "dataset": "payments_daily",
     "column": "payment_type", "type": "text"},
    {"name": "customer_country", "dataset": "payments_daily",
     "column": "customer_country", "type": "text"},
    {"name": "customer_status", "dataset": "customer_dim",
     "column": "status", "type": "text"},
]


# ---------------------------------------------------------------------------
# Row-level security — region scoping. Mirrors the governance region row filter:
# analysts see only their region; REGION_GLOBAL_GROUPS bypass. In Unity Catalog
# this is enforced on the base tables, so Power BI (DirectQuery) inherits it.
# ---------------------------------------------------------------------------
RLS = {
    "strategy": "unity_catalog_row_filter",
    "region_columns": sorted(REGION_COLUMNS),
    "scoped_group": "analysts",
    "global_groups": list(P.REGION_GLOBAL_GROUPS),
    "description": "Analysts are region-filtered by Unity Catalog; Power BI "
                   "DirectQuery inherits it. Optional Power BI RLS roles can "
                   "mirror the same region column for Import mode.",
}


# ---- accessors -------------------------------------------------------------
def dataset(name):
    return DATASET_BY_NAME[name]


def measures_for(dataset_name):
    return [m for m in MEASURES if m["dataset"] == dataset_name]


def bi_views():
    """Fully-qualified (schema.view) names of the BI serving views."""
    return ["%s.%s" % (BI_SCHEMA, d["view"]) for d in DATASETS]


def measure_catalog():
    """A serialisable catalogue of measures + dimensions for Power BI / docs."""
    return {
        "measures": [
            {k: m[k] for k in ("name", "dataset", "column", "agg", "format",
                               "description")} for m in MEASURES],
        "dimensions": [dict(d) for d in DIMENSIONS],
        "datasets": [
            {"name": d["name"], "view": "%s.%s" % (BI_SCHEMA, d["view"]),
             "source": d["source"], "rls_column": d["rls_column"]}
            for d in DATASETS],
        "rls": RLS,
    }
