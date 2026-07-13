#!/usr/bin/env bash
# [3] Azure OIDC / workload identity federation for GitHub Actions (no client secret).
# Creates an Entra app + federated credentials trusted by this repo's build branches
# and the test/prod environments, and grants AcrPush (build) + Contributor (deploy).
#
#   export GH_ORG=your-org GH_REPO=investsphere-payments
#   export SUBSCRIPTION_ID=... RG=rg-investsphere-payments ACR_NAME=investsphereacr
#   bash scripts/setup_azure_oidc.sh
# Prints the three values to store as REPO-LEVEL GitHub secrets.
set -euo pipefail

: "${GH_ORG:?}"; : "${GH_REPO:?}"; : "${SUBSCRIPTION_ID:?}"; : "${RG:?}"; : "${ACR_NAME:?}"
APP_NAME="${APP_NAME:-investsphere-github-oidc}"

APP_ID=$(az ad app create --display-name "$APP_NAME" --query appId -o tsv)
az ad sp create --id "$APP_ID" >/dev/null 2>&1 || true

add_fic () {  # name, subject
  cat <<JSON | az ad app federated-credential create --id "$APP_ID" --parameters @- >/dev/null || true
{ "name": "$1", "issuer": "https://token.actions.githubusercontent.com",
  "subject": "$2", "audiences": ["api://AzureADTokenExchange"] }
JSON
  echo "  federated credential: $2"
}

# build job runs on a branch (no environment); deploy jobs run in an environment
add_fic gh-build-main    "repo:${GH_ORG}/${GH_REPO}:ref:refs/heads/main"
add_fic gh-build-develop "repo:${GH_ORG}/${GH_REPO}:ref:refs/heads/develop"
add_fic gh-env-test      "repo:${GH_ORG}/${GH_REPO}:environment:test"
add_fic gh-env-prod      "repo:${GH_ORG}/${GH_REPO}:environment:prod"

ACR_ID=$(az acr show -n "$ACR_NAME" --query id -o tsv)
az role assignment create --assignee "$APP_ID" --role AcrPush --scope "$ACR_ID" >/dev/null

# Least-privilege deploy role instead of broad Contributor: Container Apps + Bicep only.
RG_SCOPE="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RG}"
ROLE_NAME="investsphere-deployer-${RG}"
cat > /tmp/investsphere-deployer.json <<JSON
{
  "Name": "$ROLE_NAME",
  "IsCustom": true,
  "Description": "Deploy Container Apps revisions + ARM/Bicep in this RG (no broad Contributor).",
  "Actions": [
    "Microsoft.App/containerApps/read",
    "Microsoft.App/containerApps/write",
    "Microsoft.App/containerApps/revisions/*",
    "Microsoft.App/containerApps/listSecrets/action",
    "Microsoft.App/managedEnvironments/read",
    "Microsoft.App/managedEnvironments/join/action",
    "Microsoft.Resources/deployments/*",
    "Microsoft.Resources/subscriptions/resourceGroups/read",
    "Microsoft.ContainerRegistry/registries/read"
  ],
  "AssignableScopes": ["$RG_SCOPE"]
}
JSON
az role definition create --role-definition /tmp/investsphere-deployer.json >/dev/null 2>&1 \
  || az role definition update --role-definition /tmp/investsphere-deployer.json >/dev/null
# (role propagation can lag a few seconds before the assignment succeeds)
az role assignment create --assignee "$APP_ID" --role "$ROLE_NAME" --scope "$RG_SCOPE" >/dev/null

echo
echo "Databricks OIDC (no PAT): add app $APP_ID — or the Container App's managed"
echo "identity — as a workspace user/SP with read grants on gold_*  (Settings →"
echo "Identity and access → Service principals). Runtime mints the token via MI."
echo
echo "Store these as REPO-LEVEL GitHub secrets:"
echo "  AZURE_CLIENT_ID=$APP_ID"
echo "  AZURE_TENANT_ID=$(az account show --query tenantId -o tsv)"
echo "  AZURE_SUBSCRIPTION_ID=$SUBSCRIPTION_ID"
