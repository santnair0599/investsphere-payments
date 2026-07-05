# Databricks deployment

How the project goes from the local pure-Python reference to a running Azure
Databricks workspace, using **Terraform** (infrastructure) + **Databricks Asset
Bundles** (`databricks.yml`, the jobs).

## Local reference mode vs Databricks production mode

| | Local reference mode | Databricks production mode |
|---|---|---|
| Sources | seed files + `InMemory*` clients (Oracle/SFTP/Salesforce/REST stubbed) | real JDBC / SFTP / Salesforce / REST via secret-scope creds |
| Compute | plain Python (`pipelines/smoke_test.py`, credential-free) | serverless tasks / SQL warehouse |
| Storage | in-memory lists | ADLS Gen2 + Delta in Unity Catalog |
| Orchestration | `orchestration/runner.py` executes the DAG in-process | Lakeflow Job (the same DAG, from `databricks.yml`) |
| Governance/monitoring | dict builders + validators (tested) | UC SQL (masks/filters/grants) + control/monitoring Delta tables |
| Identity | none | jobs run as the **ETL service principal** |

The reference logic is the **same code** deployed to the workspace; only the
inputs (real sources, real storage, real identity) change. The smoke test bridges
the two: it runs the production DAG wiring in reference mode (no credentials).

## Deployment order

Always deploy in this order — each layer depends on the previous:

```
1. Terraform        infra/terraform/envs/<env>   -> RG, ADLS, Key Vault, access
                                                    connector, UC catalog+schemas,
                                                    groups, cluster policy, SQL
                                                    warehouse, secret scope
2. Generated SQL    scripts/deploy_sql.sh        -> PII tags, masks, row filters,
                                                    grants, masked views, dashboards
3. Asset Bundle     scripts/deploy_bundle.sh     -> the jobs (daily_e2e + smoke)
4. Smoke test       bundle run …_smoke           -> verify wiring end-to-end
```

### 1. Terraform (infrastructure first)

```bash
terraform -chdir=infra/terraform/envs/dev init -backend-config=backend.hcl
terraform -chdir=infra/terraform/envs/dev apply
terraform -chdir=infra/terraform/envs/dev output -json   # feeds the bundle vars
```

Terraform **owns** the catalog, schemas, USE grants, groups, warehouse, cluster
policy, and the Key Vault-backed secret scope (see [TERRAFORM.md](TERRAFORM.md)).

#### Terraform outputs the bundle needs

| Terraform output | → bundle variable | used for |
|---|---|---|
| `catalog_name` | `catalog` | catalog the bundle deploys into / job param |
| `warehouse_id` | `warehouse_id` | dbt SQL warehouse for `dbt_build`/`dbt_test` |
| `secret_scope_name` | `secret_scope` | secret scope job param |
| `etl_service_principal` | `etl_service_principal` | job `run_as` in test/prod |
| `external_location_url`, `storage_account_name`, `access_connector_id`, `resource_group_name` | (runbook context) | storage paths / managed identity |

These Terraform output names must stay in sync with the bundle variables. Copy the
values into `bundle_vars/<env>.yml`.

### 2. Generated SQL (governance + dashboards, in order)

Terraform creates the schemas; the governance SQL applies the fine-grained
controls. The numeric prefixes encode dependency order:

```
0. UC schemas            (owned by Terraform; 00_catalog_schemas.sql is idempotent)
1. PII tags              governance/sql/01_pii_tags.sql
2. mask functions        governance/sql/02_mask_functions.sql
3. row filters           governance/sql/04_row_filters.sql        (after 03 masks)
4. apply masks           governance/sql/03_apply_masks.sql        (needs mask fns)
5. grants                governance/sql/05_grants.sql             (tables/views exist)
6. masked views          governance/sql/06_masked_views.sql       (last)
7. monitoring/dashboards monitoring/sql/*.sql                     (read-only)
```

```bash
scripts/deploy_sql.sh                                  # dry-run: print the plan
scripts/deploy_sql.sh --execute <warehouse_id> investsphere_dev
```

(Run order in the script is `01→02→03→04→05→06`; masks must exist before they are
applied, tables/views before grants, views last.)

### 3. Asset Bundle (the jobs)

```bash
scripts/deploy_bundle.sh validate dev          # databricks bundle validate -t dev
scripts/deploy_bundle.sh deploy   dev --dry-run   # validate only
scripts/deploy_bundle.sh deploy   dev          # databricks bundle deploy -t dev
```

The bundle defines three targets (`dev`/`test`/`prod`), the full 15-task
`investsphere_payments_daily_e2e` job, and the `investsphere_payments_smoke` job.
`dev` runs as the deploying user; **test/prod run as the ETL service principal**.

### 4. Smoke test (verify)

```bash
scripts/deploy_bundle.sh smoke dev             # runs investsphere_payments_smoke
# or locally / in CI, with no workspace:
python pipelines/smoke_test.py
```

The smoke test asserts: init runs, all Bronze sample tasks run, both gates work,
the dbt task is callable, governance validation runs, and monitoring writes the
control rows — using seed/in-memory sources, so **no Oracle/Salesforce/SFTP/REST
credentials are required**.

## Secrets & service principal

`pipelines/validate_deployment.py` is the preflight (run in CI and before deploy):

```bash
python pipelines/validate_deployment.py
```

- It checks every required source credential resolves to a secret **reference**
  (`{{secrets/<scope>/<key>}}`) — it prints the **names** (`rest-api-token`,
  `sftp-user`, `sftp-password`, `salesforce-client-id`, `salesforce-client-secret`),
  **never values**.
- It asserts `test`/`prod` jobs `run_as` the ETL service principal
  (`${var.etl_service_principal}`), and that the required job parameters exist.

Real secret values are set **out of band** (never in git or Terraform state):

```bash
az keyvault secret set --vault-name kv-investpay-dev --name salesforce-client-secret --value '***'
```

### Required permissions

| Identity | Needs |
|---|---|
| Terraform deployer (Azure) | Contributor on the RG + UC account admin to create catalog/credential/external location |
| Bundle deployer (dev) | workspace `CAN_MANAGE` on the bundle's jobs; `USE CATALOG` on the dev catalog |
| **ETL service principal** (`spn_investsphere_etl`, test/prod run-as) | `USE CATALOG`; `MODIFY`+`SELECT` on Bronze/Silver/Gold base tables (the only writer); `READ` on the secret scope; `CAN_USE` on the SQL warehouse |
| Analysts / engineers / stewards | least-privilege grants from the governance model (no PII base read) — see [GOVERNANCE.md](GOVERNANCE.md) |

The secret scope grants `READ` to the ETL service principal only.

## CI/CD

- **`ci.yml`** (every PR, no cloud creds): dbt parse, generated-SQL drift
  (governance + monitoring + BI + terraform grants must be regenerated and
  committed), terraform fmt+validate, deploy preflight, smoke test, and a
  best-effort `databricks bundle validate`.
- **`terraform.yml`**: fmt/validate on PRs; a manual, environment-gated `plan`.
- **`deploy.yml`**: manual `workflow_dispatch` bundle deploy; **prod is gated behind
  a GitHub Environment approval** (configure required reviewers on the `prod`
  environment). Deployment is never automatic on push.

## Runbook — first dev deployment

```bash
# 0. prerequisites: databricks CLI authenticated to the dev workspace, az logged in
# 1. infrastructure
terraform -chdir=infra/terraform/envs/dev init -backend-config=backend.hcl
terraform -chdir=infra/terraform/envs/dev apply
terraform -chdir=infra/terraform/envs/dev output -raw warehouse_id   # copy this

# 2. fill bundle_vars/dev.yml  -> set warehouse_id (catalog/scope/SP already defaulted)

# 3. secrets (out of band)
az keyvault secret set --vault-name kv-investpay-dev --name rest-api-token --value '***'
#   …repeat for sftp-user/-password, salesforce-client-id/-secret

# 4. governance + dashboard SQL
scripts/deploy_sql.sh --execute "$(terraform -chdir=infra/terraform/envs/dev output -raw warehouse_id)" investsphere_dev

# 5. preflight + bundle
python pipelines/validate_deployment.py
scripts/deploy_bundle.sh validate dev
scripts/deploy_bundle.sh deploy   dev

# 6. verify
scripts/deploy_bundle.sh smoke dev
#   then run the real pipeline:
scripts/deploy_bundle.sh run dev
```

Promotion to **test** then **prod** repeats steps 1–6 with `-t test` / `-t prod`;
prod deploys go through the `deploy.yml` approval gate.
