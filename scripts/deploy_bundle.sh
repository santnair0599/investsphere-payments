#!/usr/bin/env bash
#
# Databricks Asset Bundle deploy helper.
#
#   scripts/deploy_bundle.sh validate dev
#   scripts/deploy_bundle.sh deploy   dev            # deploy to the dev target
#   scripts/deploy_bundle.sh deploy   dev --dry-run  # validate only, no deploy
#   scripts/deploy_bundle.sh run      dev            # run the daily_e2e job
#   scripts/deploy_bundle.sh smoke    dev            # run the smoke-test job
#
# Bundle variable values are read from bundle_vars/<target>.yml (filled from
# `terraform output`) and passed as --var. Requires the `databricks` CLI v0.2xx+
# authenticated to the target workspace (DATABRICKS_HOST/TOKEN or a profile).
set -euo pipefail

ACTION="${1:?usage: deploy_bundle.sh <validate|deploy|run|smoke> <target> [--dry-run]}"
TARGET="${2:-dev}"
FLAG="${3:-}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VARFILE="$ROOT/bundle_vars/${TARGET}.yml"
DAILY_JOB="investsphere_payments_daily_e2e"
SMOKE_JOB="investsphere_payments_smoke"

if [[ ! -f "$VARFILE" ]]; then
  echo "ERROR: no bundle_vars file for target '$TARGET' ($VARFILE)" >&2
  exit 2
fi

# Build --var flags from the bundle-var keys in the var file (values only; the
# file never contains secret values — warehouse_id/catalog/scope/SP only).
VARS=()
for key in catalog secret_scope etl_service_principal warehouse_id; do
  val="$(grep -E "^${key}:" "$VARFILE" | head -1 | sed -E "s/^${key}:[[:space:]]*//; s/[[:space:]]*#.*$//; s/^\"//; s/\"$//; s/[[:space:]]*$//")"
  if [[ -n "$val" ]]; then
    VARS+=(--var "${key}=${val}")
  fi
done

echo "==> bundle action='$ACTION' target='$TARGET' vars=${VARS[*]:-<none>}"

run_validate() { databricks bundle validate -t "$TARGET" "${VARS[@]:-}"; }

case "$ACTION" in
  validate)
    run_validate ;;
  deploy)
    run_validate
    if [[ "$FLAG" == "--dry-run" || "$FLAG" == "--validate-only" ]]; then
      echo "==> dry-run: validated only, NOT deploying."
    else
      databricks bundle deploy -t "$TARGET" "${VARS[@]:-}"
    fi ;;
  run)
    databricks bundle run "$DAILY_JOB" -t "$TARGET" "${VARS[@]:-}" ;;
  smoke)
    databricks bundle run "$SMOKE_JOB" -t "$TARGET" "${VARS[@]:-}" ;;
  *)
    echo "ERROR: unknown action '$ACTION'" >&2
    exit 2 ;;
esac
