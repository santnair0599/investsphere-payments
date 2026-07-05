#!/usr/bin/env bash
#
# Deploy the generated Unity Catalog governance SQL (and list the monitoring
# dashboard SQL) in the REQUIRED order against a SQL warehouse.
#
#   scripts/deploy_sql.sh                         # dry-run: print the ordered plan
#   scripts/deploy_sql.sh --execute <warehouse_id> [catalog]
#
# Deployment order (see docs/DATABRICKS_DEPLOYMENT.md):
#   0. Unity Catalog schemas      <- owned by TERRAFORM (already applied); the
#                                    governance 00_catalog_schemas.sql is idempotent.
#   1. PII tags                   governance/sql/01_pii_tags.sql
#   2. mask functions             governance/sql/02_mask_functions.sql
#   3. apply masks                governance/sql/03_apply_masks.sql
#   4. row filters                governance/sql/04_row_filters.sql
#   5. grants                     governance/sql/05_grants.sql
#   6. masked views               governance/sql/06_masked_views.sql
#   7. monitoring / dashboard SQL monitoring/sql/*.sql  (dashboard datasets/alerts)
#
# The numeric filename prefixes encode dependency order — mask functions must
# exist before masks are applied; tables/views before grants; views last.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GOV_DIR="$ROOT/governance/sql"
MON_DIR="$ROOT/monitoring/sql"
BI_DIR="$ROOT/bi/sql"

MODE="dry-run"
WAREHOUSE_ID=""
CATALOG="${3:-investsphere}"
if [[ "${1:-}" == "--execute" ]]; then
  MODE="execute"
  WAREHOUSE_ID="${2:?--execute requires a warehouse_id}"
fi

# Regenerate so what we deploy matches the policy model (drift guard).
python "$ROOT/pipelines/generate_governance_sql.py" >/dev/null
python "$ROOT/pipelines/generate_monitoring_sql.py" "$CATALOG" >/dev/null
python "$ROOT/pipelines/generate_bi_sql.py" "$CATALOG" >/dev/null

run_sql_file() {
  local file="$1"
  if [[ "$MODE" == "dry-run" ]]; then
    echo "    [dry-run] would execute: ${file#$ROOT/}"
  else
    echo "    executing: ${file#$ROOT/}"
    databricks api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"statement\":$(python -c 'import json,sys;print(json.dumps(open(sys.argv[1],encoding="utf-8").read()))' "$file"),\"wait_timeout\":\"30s\"}"
  fi
}

echo "==> Governance SQL (ordered) — catalog=$CATALOG mode=$MODE"
echo "  0. schemas: owned by Terraform (00_catalog_schemas.sql is idempotent)"
for f in 00_catalog_schemas 01_pii_tags 02_mask_functions 03_apply_masks \
         04_row_filters 05_grants 06_masked_views; do
  run_sql_file "$GOV_DIR/${f}.sql"
done

echo "==> BI serving views (after governance masked views; analysts read these)"
for f in 00_v_payments_daily_bi 01_v_payments_fact_bi 02_v_customer_bi 03_bi_grants; do
  run_sql_file "$BI_DIR/${f}.sql"
done

echo "==> Monitoring / dashboard SQL (read-only; wire as Databricks SQL dashboards/alerts)"
for f in "$MON_DIR"/*.sql; do
  run_sql_file "$f"
done

echo "==> done ($MODE)."
