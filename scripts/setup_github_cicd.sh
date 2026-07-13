#!/usr/bin/env bash
# [1] Configure GitHub environments, secrets, and branch protection via the gh CLI.
# Prereq: `gh auth login`, and export the secret values (or `source` a .env) first.
#
#   export GH_REPO=your-org/investsphere-payments PROD_REVIEWER=your-github-login
#   # repo-level (shared): AZURE_CLIENT_ID AZURE_TENANT_ID AZURE_SUBSCRIPTION_ID ACR_NAME ACR_LOGIN_SERVER
#   # per-env: TEST_AZURE_RG, PROD_AZURE_RG, TEST_DATABRICKS_HOST, ... (PREFIX with TEST_/PROD_)
#   bash scripts/setup_github_cicd.sh
set -euo pipefail
: "${GH_REPO:?e.g. org/investsphere-payments}"

echo "== environments =="
gh api -X PUT "repos/$GH_REPO/environments/test" >/dev/null
REVIEWER_ID=$(gh api "users/${PROD_REVIEWER:?set PROD_REVIEWER}" --jq .id)
cat <<JSON | gh api -X PUT "repos/$GH_REPO/environments/prod" --input - >/dev/null
{ "reviewers": [{"type": "User", "id": $REVIEWER_ID}],
  "deployment_branch_policy": {"protected_branches": true, "custom_branch_policies": false} }
JSON
echo "  test (auto) + prod (reviewer: $PROD_REVIEWER)"

echo "== secrets =="
for k in AZURE_CLIENT_ID AZURE_TENANT_ID AZURE_SUBSCRIPTION_ID ACR_NAME ACR_LOGIN_SERVER; do
  gh secret set "$k" -R "$GH_REPO" -b "${!k:?set $k}"; done
for env in test prod; do
  PFX=$(echo "$env" | tr '[:lower:]' '[:upper:]')
  for k in AZURE_RG DATABRICKS_HOST DATABRICKS_HTTP_PATH DATABRICKS_CATALOG \
           AZURE_OPENAI_ENDPOINT AZURE_OPENAI_API_KEY AZURE_OPENAI_DEPLOYMENT AZURE_OPENAI_API_VERSION \
           AZURE_SEARCH_ENDPOINT AZURE_SEARCH_KEY AZURE_SEARCH_AGENT AZURE_AI_PROJECT TEAMS_WEBHOOK_URL; do
    var="${PFX}_$k"
    if [ -n "${!var:-}" ]; then gh secret set "$k" -R "$GH_REPO" --env "$env" -b "${!var}"; fi
  done
done

echo "== branch protection (require ai-quality-gate) =="
for br in main develop; do
  cat <<JSON | gh api -X PUT "repos/$GH_REPO/branches/$br/protection" --input - >/dev/null
{ "required_status_checks": {"strict": true, "contexts": ["ai-quality-gate"]},
  "enforce_admins": true,
  "required_pull_request_reviews": {"required_approving_review_count": 1},
  "restrictions": null }
JSON
  echo "  $br: ai-quality-gate required"
done
echo "Done."
