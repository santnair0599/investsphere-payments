# CI/CD setup — AI plane gates & deploy

How the workflows fit together, and the one-time GitHub config they need.

## Workflow map
```
PR / push
  └─ ci.yml
       ├─ dbt-parse · generated-sql-drift · terraform-validate · bundle
       ├─ ai-quality-gate      (offline, deterministic; REQUIRED check → blocks merge)
       │     └─ uploads quality-gate-report artifact (always)
       ├─ deploy-test          (push→develop, needs: ai-quality-gate)
       │     └─ uses: ai-deploy.yml (environment: test)  → build · provision · index · smoke test
       └─ deploy-prod          (push→main,   needs: ai-quality-gate)
             └─ uses: ai-deploy.yml (environment: prod)  → PAUSES for required-reviewer approval, then deploys

concurrency: PR runs for the same ref cancel superseded runs; push runs never cancel (no interrupted deploys).

nightly (02:00 UTC)
  └─ ai-nightly-gate.yml       (LIVE Azure/Databricks; uploads evidence always; Teams alert on fail)

manual
  └─ ai-deploy.yml (workflow_dispatch, environment: test|prod)
  └─ deploy.yml    (Asset Bundle, environment-gated)
```

## 0. Scripted setup (do this once)
```bash
# Azure OIDC identity + federated creds + RBAC (no client secret):
GH_ORG=you GH_REPO=investsphere-payments SUBSCRIPTION_ID=... RG=... ACR_NAME=... \
  bash scripts/setup_azure_oidc.sh          # prints AZURE_CLIENT_ID/TENANT/SUBSCRIPTION

# GitHub environments + secrets + branch protection:
GH_REPO=you/investsphere-payments PROD_REVIEWER=you \
  AZURE_CLIENT_ID=... AZURE_TENANT_ID=... AZURE_SUBSCRIPTION_ID=... ACR_NAME=... ACR_LOGIN_SERVER=... \
  TEST_AZURE_RG=... PROD_AZURE_RG=... TEST_DATABRICKS_HOST=... (etc, TEST_/PROD_ prefixed) \
  bash scripts/setup_github_cicd.sh
```
The sections below are what those scripts configure (do them by hand if you prefer).

## 1. Branch protection (makes the gate actually block)
Repo → **Settings → Branches → Add rule** for `main` and `develop`:
- ☑ Require status checks to pass → add **`ai-quality-gate`** (and the other `ci.yml` jobs).
- ☑ Require branches to be up to date.

## 2. GitHub Environments
Repo → **Settings → Environments** → create **`test`** and **`prod`**.
- **prod** → add **Required reviewers** (manual approval before any prod deploy).
- **test** → optional reviewers; this is where nightly + deploy-test pull creds.

## 3. Secrets — OIDC (no stored SP secret)
Auth is **Azure OIDC / workload identity federation** — there is **no `AZURE_CREDENTIALS`**.

**Repo-level** (shared build identity + shared registry):
| Secret | Used by |
|--------|---------|
| `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_SUBSCRIPTION_ID` | `azure/login` OIDC (build + deploy) |
| `ACR_NAME`, `ACR_LOGIN_SERVER` | build-once image push |

**Per-environment** (`test` and `prod`):
| Secret | Used by |
|--------|---------|
| `AZURE_RG` | Bicep provision, revision/traffic ops |
| `AZURE_OPENAI_ENDPOINT` / `_API_KEY` / `_DEPLOYMENT` / `_API_VERSION` | agent + eval + indexing |
| `AZURE_SEARCH_ENDPOINT` / `_KEY` / `AZURE_SEARCH_AGENT` | RAG + agentic retrieval |
| `AZURE_AI_PROJECT` | red-team simulator + Foundry evals |
| `DATABRICKS_HOST` / `_HTTP_PATH` / `_TOKEN` / `_CATALOG` | Gold marts (read-only) |
| `TEAMS_WEBHOOK_URL` | nightly failure alert |

> The **offline** `ai-quality-gate` needs **none** of these — it runs on deterministic
> fallbacks, so every PR is gated with zero secrets. Live values are only consumed by
> `build`, `deploy-test`/`deploy-prod` (via `ai-deploy.yml`), and the nightly workflow.

## 5. Delivery mechanics
- **Build once / promote by digest**: `build` pushes the image and outputs the immutable
  `…@sha256:` digest; both deploys use that **same digest**.
- **Prod ships the test-approved digest**: on `main`, `deploy-prod` `needs: deploy-test`,
  so prod runs only after the same-run test deploy + smoke passed, shipping the identical
  digest test just validated.
- **Blue-green + canary soak + rollback**: each deploy rolls a *green* revision at **0%
  traffic**, smoke-tests it directly, then shifts (`canary_percent`: test=100 cutover,
  prod=20 canary). Prod then **soaks** `soak_seconds` while health-probing green and, if
  healthy, **auto-promotes to 100%** and deactivates blue; if not, **rolls back to blue**.
  Any earlier failure also rolls back — no user impact.
- **No stored Databricks PAT**: the Container App uses its **managed identity** to mint a
  short-lived Entra token for Databricks at runtime (`ai/tools/databricks_client._aad_token`);
  add that MI as a workspace SP with `gold_*` read grants.
- **Least-privilege deploy identity**: OIDC federated app holds `AcrPush` + a **custom
  `investsphere-deployer` role** (Container Apps + Bicep only) — **not** Contributor.

## 4. Verify locally before pushing
```bash
python -m ai.ci.run_quality_gate      # offline gate + writes ai/ci/_gate_report.json
python -m ai.ci.smoke_deploy --base-url https://<app-fqdn>   # after a deploy
```
