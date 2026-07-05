"""
Generate the BI **serving views** from the semantic model.

All views are created in ``gold_marts`` (the analyst-granted schema) and source
only marts / masked views / the non-PII fact — so Power BI connecting as the
``analysts`` group inherits Unity Catalog masking + the region row filter.

``generate_all(catalog)`` returns ``{filename: sql}``; the views are owned by the
ETL service principal (which holds SELECT on the bases), and analysts are granted
SELECT on the views only. Run after the governance SQL (masks/filters/views) —
see docs/POWER_BI.md and docs/DATABRICKS_DEPLOYMENT.md.
"""
from __future__ import annotations

from payments_platform.bi import semantic as S

DEFAULT_CATALOG = "investsphere"


def _fqn(catalog, schema_object):
    return "%s.%s" % (catalog, schema_object)


def gen_payments_daily(catalog):
    src = _fqn(catalog, "gold_marts.daily_payment_summary")
    view = _fqn(catalog, "%s.v_payments_daily_bi" % S.BI_SCHEMA)
    return (
        "-- Daily payments BI dataset (passthrough of the Gold mart; no PII)\n"
        "CREATE OR REPLACE VIEW %s AS\n"
        "SELECT transaction_date, currency_code, payment_type, customer_country,\n"
        "       payment_count, total_amount, avg_amount\n"
        "FROM %s;\n" % (view, src))


def gen_payments_fact(catalog):
    fact = _fqn(catalog, "gold.fact_payments")
    dim = _fqn(catalog, "gold.dim_customer")
    view = _fqn(catalog, "%s.v_payments_fact_bi" % S.BI_SCHEMA)
    # fact has no PII; join only dim_customer.nationality for region scoping.
    return (
        "-- Payment-grain BI fact (no PII), enriched with customer_country for RLS\n"
        "CREATE OR REPLACE VIEW %s AS\n"
        "SELECT f.payment_id, f.customer_id, f.account_id, f.amount,\n"
        "       f.currency_code, f.payment_type, f.transaction_date,\n"
        "       c.nationality AS customer_country, f.source_system\n"
        "FROM %s f\n"
        "LEFT JOIN %s c ON f.customer_id = c.customer_id;\n" % (view, fact, dim))


def gen_customer_dim(catalog):
    src = _fqn(catalog, "gold_masked.v_customer_masked_for_analytics")
    view = _fqn(catalog, "%s.v_customer_bi" % S.BI_SCHEMA)
    return (
        "-- Customer dimension for BI: sources the masked view (PII display-masked)\n"
        "CREATE OR REPLACE VIEW %s AS\n"
        "SELECT customer_id, nationality, status, source_system,\n"
        "       customer_name, email, phone_number\n"   # already masked upstream
        "FROM %s;\n" % (view, src))


def gen_grants(catalog):
    """Grant analysts SELECT on the BI views only (least privilege)."""
    lines = ["-- Analysts read the BI serving views only (not the Gold base)."]
    for d in S.DATASETS:
        view = _fqn(catalog, "%s.%s" % (S.BI_SCHEMA, d["view"]))
        for group in d["audience"]:
            lines.append("GRANT SELECT ON VIEW %s TO `%s`;" % (view, group))
    return "\n".join(lines) + "\n"


def generate_all(catalog=DEFAULT_CATALOG):
    """Return ``{filename: sql}`` for the BI serving layer (ordered)."""
    return {
        "00_v_payments_daily_bi.sql": gen_payments_daily(catalog),
        "01_v_payments_fact_bi.sql": gen_payments_fact(catalog),
        "02_v_customer_bi.sql": gen_customer_dim(catalog),
        "03_bi_grants.sql": gen_grants(catalog),
    }
