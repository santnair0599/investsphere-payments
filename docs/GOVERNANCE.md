# Unity Catalog governance — InvestSphere Payments

Governance is **policy-as-code**: one declarative model
(`src/payments_platform/governance/policy.py`) is the single source of truth.
From it we **generate** the Databricks SQL and **validate** the security posture
with tests — so the SQL that ships and the tests that guard it can never drift.

```
policy.py  ──┬──►  sql_generator.py  ──►  governance/sql/*.sql       (deploy to UC)
             └──►  validate.py        ──►  governance_validation task (runtime gate)
```

`validate.py` runs for real as the **`governance_validation`** Lakeflow task
(`pipelines/dag_task.py`) and **fails the job (exit 1)** on any PII/access
violation — the gate is enforced on every Databricks run.

## Generate & deploy

```bash
python pipelines/generate_governance_sql.py     # writes governance/sql/*.sql
```

Run the SQL **in order** against the workspace (Databricks SQL editor, a notebook,
or `databricks sql`), after the groups exist:

| File | Purpose | Maps to UC feature |
|---|---|---|
| `00_catalog_schemas.sql` | catalog + schemas (incl. `governance`, `gold_masked`) | `CREATE CATALOG/SCHEMA` |
| `01_pii_tags.sql` | classify PII columns | UC **tags** (`pii`, `classification`) |
| `02_mask_functions.sql` | mask UDFs (name/email/phone/id) | SQL UDFs |
| `03_apply_masks.sql` | attach masks to PII columns | `ALTER COLUMN … SET MASK` |
| `04_row_filters.sql` | region row filter + attach | `SET ROW FILTER` |
| `05_grants.sql` | least-privilege grants | `GRANT … TO` |
| `06_masked_views.sql` | analyst (masked-for-analytics) + engineer views | masked `VIEW`s |

## Groups (account-level)

`data_engineers`, `analysts`, `pii_approved_users`, `data_stewards`,
`spn_investsphere_etl`. **Groups are not created in SQL** — provision them via
**SCIM / Entra ID / Terraform** (`databricks_group` + `databricks_group_member`).
The generated grants reference them by name.

## Access matrix

| Object | ETL SP | data_engineers | analysts | pii_approved | data_stewards |
|---|---|---|---|---|---|
| Bronze/Silver/Gold base (read+write) | ✅ RW | — | — | — | — |
| PII base (`customer_scd2`, `dim_customer*`) | RW | ❌ | ❌ | ✅ SELECT (unmasked) | — |
| Non-PII Gold (`fact_payments`) | RW | ✅ SELECT | — | — | — |
| Gold marts (`daily_payment_summary`) | RW | ✅ | ✅ | — | — |
| Masked view — engineer (hashed keys) | — | ✅ | — | — | — |
| Masked view — analytics (`v_customer_masked_for_analytics`) | — | — | ✅ | — | — |
| Quarantine raw payload | RW | ❌ | ❌ | ✅ | ✅ |
| Control tables | RW | ✅ | — | — | ✅ |

The **only writer is the service principal** — engineers/analysts never write
production tables.

## How masking works

Column masks are UDFs that reveal the real value **only** to
`pii_approved_users`, otherwise return a masked form:

| Class | Columns | Masked form |
|---|---|---|
| `email` | email | `s***@x.com` (first char + domain) |
| `phone` | phone_number | `XXXXXX1234` (last 4) |
| `name` | customer_name | `S***` (initial) |
| `national_id` | national_id, emirates_id, passport_number | `sha2(…, 256)` |

Masks are attached to the **base** PII tables. The **masked views** additionally
transform PII (display-mask for analysts; `sha2` hashes for engineers so
they can debug joins on `email_hash` without seeing the address) and **drop**
the high-sensitivity ID columns entirely — defense in depth: even if a view's
grants were widened, no raw PII is in the projection.

## Row-level security

`region_filter(region)` returns all rows to global groups
(`pii_approved_users`, `data_stewards`, ETL) and otherwise matches a
`region_<country>` group (e.g. add a `region_uae` UC group → that team sees only
`nationality = 'UAE'`). Attached to `customer_scd2`, `dim_customer*` (on
`nationality`) and the mart (on `customer_country`).

## Quarantine lockdown

`silver_quarantine.failed_records` holds **raw payloads** of rejected records, so
it's treated as sensitive: only `data_stewards`, `pii_approved_users` and the ETL
SP can read it. Engineers see DQ *metrics* (counts/rules) via control tables, not
the raw payload.

## What the governance gate enforces

`validate.py` (the `governance_validation` task) fails the run if any of these become true:

1. `data_engineers` get direct `SELECT` on a PII base table.
2. A PII column has no mask function.
3. Quarantine raw payload is exposed to a non-privileged group.
4. A masked view exposes a raw PII column (explicitly, or by passing one through
   un-masked/un-hashed/un-dropped).
5. `analysts` get `SELECT` on any base table (must use marts/masked views).

Each check is also tested against a deliberately-broken model, proving the guard
actually trips — not just that the current model happens to pass.

## Mapping to production Unity Catalog

- **Tags** → UC governed tags; pair with **Data Classification** (auto-discovery
  of PII) to keep the `pii` set current.
- **Masks / row filters** → exactly the UC `SET MASK` / `SET ROW FILTER`
  primitives (or ABAC policies / dynamic views).
- **Grants** → manage via Terraform `databricks_grants` for drift-free, reviewed
  changes; the generated SQL is the human-readable equivalent.
- **Ownership** → set table/schema owners to a controlled data-owner or the ETL
  SP, **not** `data_engineers` (owners can change policies).
- **Secrets / storage** → JDBC/Kafka/API creds in an Azure Key Vault-backed
  secret scope; ADLS access via a managed identity / external locations +
  storage credentials, never per-user keys (so users can't bypass UC by hitting
  ADLS directly).
- **Audit** → monitor `system.access.audit` for PII-table queries, mask/grant
  changes, and quarantine access (see `docs/DESIGN.md` monitoring section).
