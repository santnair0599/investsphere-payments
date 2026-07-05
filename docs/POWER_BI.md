# Power BI / BI consumption layer

The serving layer Power BI (or any BI tool) connects to. It is **policy-as-code**
(`src/payments_platform/bi/`): a declarative semantic model + generated serving
views that are **analyst-safe by construction** — every dataset sources a Gold
mart, a governance masked view, or the non-PII fact, so Power BI inherits Unity
Catalog masking and the region row filter automatically.

```
src/payments_platform/bi/
  semantic.py       datasets, measures (KPIs), dimensions, RLS — references real Gold columns
  sql_generator.py  CREATE OR REPLACE VIEW serving views (catalog-parameterised) + grants
  validate.py       no-PII / analyst-safe-source / valid-column / RLS-alignment checks
pipelines/generate_bi_sql.py   -> bi/sql/*.sql + bi/measures.json
```

```bash
python pipelines/generate_bi_sql.py investsphere_dev   # generate views + measure catalogue
```

## Serving views (what BI reads)

All created in `gold_marts` (the analyst-granted schema), owned by the ETL
service principal, with analysts granted SELECT on the **views only** — never the
Gold base:

| BI view (`gold_marts.*`) | Sources | Why it's safe |
|---|---|---|
| `v_payments_daily_bi` | `gold_marts.daily_payment_summary` | pre-aggregated mart, no PII |
| `v_payments_fact_bi` | `gold.fact_payments` (+ `dim_customer.nationality` for RLS) | fact has no PII |
| `v_customer_bi` | `gold_masked.v_customer_masked_for_analytics` | PII already display-masked |

`validate.py` proves no view exposes a raw PII column and none sources a PII base
table (`gold.dim_customer` / `silver_cdc.customer_scd2`).

## Semantic model (`bi/measures.json`)

**Measures (KPIs):** `total_payment_amount` (SUM), `payment_count` (SUM),
`avg_payment_amount` (AVG), `active_customers` (DISTINCTCOUNT), `distinct_currencies`.
**Dimensions:** transaction_date, currency, payment_type, customer_country,
customer_status. Each measure/dimension is validated to reference a real column in
its dataset. Import `bi/measures.json` to scaffold the Power BI dataset; the
measures translate directly to DAX, e.g.:

```DAX
Total Payment Amount = SUM(v_payments_daily_bi[total_amount])
Active Customers      = DISTINCTCOUNT(v_payments_fact_bi[customer_id])
```

## Connecting Power BI to Databricks

Use the native **Azure Databricks / Spark connector** pointed at the **SQL
warehouse** (`terraform output warehouse_id` → the bundle's `warehouse_id`).
Authenticate with the **`analysts`** group identity (AAD/Entra passthrough or a
service principal that is a member of `analysts`).

| Mode | When | Governance behaviour |
|---|---|---|
| **DirectQuery** | live data, governance-critical reports | **Recommended.** Every query runs in UC as the analyst, so column masks + the region row filter are enforced at query time — Power BI physically cannot see unmasked PII or other regions. |
| **Import** | high-performance dashboards, smaller marts | Data is cached in Power BI, so connect **only to the masked/mart serving views** (which is all this layer exposes) and optionally mirror the region filter as a Power BI **RLS role** on `customer_country` / `nationality`. |

### Row-level security

Governance applies a region row filter on `nationality` (customer) /
`customer_country` (mart); `analysts` are scoped, `pii_approved_users` /
`data_stewards` / the ETL SP bypass. In **DirectQuery** this is inherited from UC
with no Power BI config. For **Import**, add a Power BI RLS role filtering the same
region column so the cached copy matches. `validate.py` asserts the BI RLS columns
equal the governance region columns, so the two can't drift.

## Deployment order

The BI views slot in **after** the governance SQL (they depend on the masked views
and grants) and after the Gold tables exist:

```
Terraform -> generated governance SQL -> Gold (dbt) -> BI serving SQL -> Power BI
```

```bash
# after governance + dbt Gold are deployed:
python pipelines/generate_bi_sql.py investsphere_dev
#   run bi/sql/00..03 against the SQL warehouse (same pattern as scripts/deploy_sql.sh)
```

## Interview-ready summary

The BI layer is a thin, governed **serving contract** over Gold, not a second copy
of the data. Three principles:

1. **Safe by construction.** BI never touches raw PII — datasets source marts,
   masked views, or the non-PII fact, and a policy-as-code validator (run in the
   generator and CI) fails the build if a view would expose a PII column or source
   a PII base table. Analysts hold SELECT on the serving views only.
2. **Governance inherited, not re-implemented.** With DirectQuery, Unity Catalog
   enforces column masking and the region row filter at query time for the
   `analysts` group, so the same policy protects SQL, notebooks, and Power BI
   identically — the BI tool can't be the weak link.
3. **A semantic model as code.** Measures, dimensions, and RLS are declared once
   (`semantic.py`), validated against real columns, and exported to
   `bi/measures.json` for Power BI — so the metric definitions live in version
   control next to the pipeline, not only inside a .pbix file.

Performance levers (see also [PERFORMANCE_COST.md](PERFORMANCE_COST.md)): serve
pre-aggregated marts to keep dashboard queries cheap, DirectQuery against a
**Photon** SQL warehouse, and **Liquid Clustering** on the large fact so BI filter
predicates (date / currency / country) prune efficiently.
