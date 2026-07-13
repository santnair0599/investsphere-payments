# Terraform — platform foundation

> This project evolved from a payments-practice foundation into an enterprise
> business AI decision platform. The original ingestion and lakehouse patterns were
> preserved and generalized across enterprise domains (real estate, hospitality,
> entertainment, investment, customer/CRM, ops-trust).

The `infra/terraform/` layer provisions the **production platform foundation** for
the InvestSphere enterprise lakehouse on Azure Databricks: the Azure resources, Unity Catalog
catalog/schemas/grants, identity groups, compute policy + SQL warehouse, and the
Key Vault-backed secret scope. It is **additive** — it provisions *where* the
platform runs; the application code, jobs, and DLT/dbt pipelines are deployed
separately by the Databricks **Asset Bundle** (`databricks.yml`).

```
infra/terraform/
  modules/
    azure_foundation/   # resource group, ADLS Gen2 + medallion containers, Key Vault, access connector
    identity/           # account-level UC groups (5)
    unity_catalog/      # storage credential, external location, catalog, schemas, USE grants
    compute/            # job cluster policy + SQL warehouse (cost controls, tags)
    secrets/            # KV-backed secret scope + source-credential placeholders
  envs/
    dev/  test/  prod/  # one root module per environment (main/variables/providers/backend/tfvars)
  generated/            # schemas.auto.tfvars.json + grants.auto.tfvars.json  (DO NOT hand-edit)
```

## Governance is the source of truth — grants are generated, never hand-written

The grant and schema lists Terraform applies are **derived from the governance
policy-as-code model** (`src/payments_platform/governance/policy.py`), not
maintained by hand:

```
python pipelines/generate_terraform_grants.py
  -> infra/terraform/generated/schemas.auto.tfvars.json   {"schemas": [...]}
  -> infra/terraform/generated/grants.auto.tfvars.json    {catalog_grants, schema_grants}
```

Each env's `main.tf` `jsondecode(file(...))`s those files, so the infra grants can
**never drift** from the security policy. CI enforces this:

- The `terraform` workflow runs `pipelines/generate_terraform_grants.py` and
  `git diff --exit-code infra/terraform/generated`, so a stale generated file fails
  the build.
- The policy model itself guarantees no schema-level path to a PII/raw schema
  (`bronze`, `silver_clean`, `silver_cdc`, `silver_quarantine`) is granted to
  `data_engineers` or `analysts`.

Least-privilege summary (enforced, not aspirational):

| Principal              | Catalog     | Notable schema access                              | PII base tables |
|------------------------|-------------|----------------------------------------------------|-----------------|
| `spn_investsphere_etl` | USE CATALOG | all (read/write base — the only writer)            | yes (ETL only)  |
| `data_engineers`       | USE CATALOG | `gold`, `gold_marts`, `gold_masked`, `monitoring`, `silver_control` | **no** |
| `analysts`             | USE CATALOG | `gold_marts`, `gold_masked` (masked views only)    | **no**          |
| `pii_approved_users`   | USE CATALOG | `gold`, `silver_cdc` (unmasked PII)                | yes             |
| `data_stewards`        | USE CATALOG | `silver_quarantine`, `silver_control`, `monitoring`| no (quarantine review) |

Table / column-mask / row-filter / view grants are **not** owned here — see the
ownership split below.

## What Terraform owns vs what the Asset Bundle owns

| Concern | Owner | Why |
|---|---|---|
| Resource group, ADLS Gen2, medallion containers, Key Vault | **Terraform** | long-lived cloud infra |
| Databricks access connector / managed identity + role assignment | **Terraform** | identity to reach storage |
| UC storage credential + external location | **Terraform** | binds UC to the connector |
| Catalog + schemas | **Terraform** | the namespace the bundle deploys *into* |
| Account groups (5) | **Terraform** | identity, synced via SCIM |
| Catalog/schema **USE** grants | **Terraform** | coarse access path, policy-derived |
| Job cluster **policy** + SQL warehouse | **Terraform** | shared compute guardrails + cost controls |
| KV-backed secret **scope** + placeholders | **Terraform** | scope is infra; values set out of band |
| **Table / column-mask / row-filter / masked-view** grants | **Governance SQL** (`governance/sql/`, slice 2) | fine-grained, generated from same policy |
| Jobs / tasks / DAG, DLT pipelines, dbt models, notebooks/wheels | **Asset Bundle** (`databricks.yml`) | application code, deployed per-commit |
| Job *parameters* (env, catalog, run_date, secret_scope) | **Asset Bundle** | runtime, not infra |

Rule of thumb: **Terraform builds the house and hands the bundle the keys**
(catalog name, warehouse id, cluster-policy id, secret scope). The bundle moves
in. Terraform changes are rare and gated; bundle deploys happen on every merge.

The bundle consumes Terraform outputs by name — e.g. it deploys into
`investsphere_<env>`, references the secret scope `investsphere_payments`, and
targets the SQL warehouse Terraform created. Those names are stable contracts,
not duplicated config.

## Secrets

The `secrets` module creates a Key Vault-backed Databricks secret scope and
placeholder keys for every source credential the ingestors read via
`config.secrets.get_secret(scope, key)`:

```
oracle-jdbc-user / -password   sqlserver-jdbc-user / -password
salesforce-client-id / -secret sftp-user / -password   rest-api-token
```

Real values are **never** in Terraform state. Lower envs may create `REPLACE_ME`
placeholders (`create_placeholder_values = true` in dev/test); prod sets
`create_placeholder_values = false` and values are written out of band:

```bash
az keyvault secret set --vault-name kv-investpay-prod --name oracle-jdbc-password --value '***'
```

## dev / test / prod promotion

Each environment is an **independent root module** with its own `backend.tf` state
key (`investsphere-payments/<env>.tfstate`) and `terraform.tfvars.json`. The module
graph is identical across envs (the three `main.tf` files are byte-identical); only
the tfvars differ — subscription, names, region, and cost knobs:

| | dev | test | prod |
|---|---|---|---|
| warehouse size | Small | Small | Medium |
| auto-stop (min) | 10 | 10 | 5 |
| secret placeholders | yes | yes | **no** |
| catalog | `investsphere_dev` | `investsphere_test` | `investsphere_prod` |

Promotion is **the same code, a different tfvars + state**, advanced through the CI
gate:

1. Merge to `main` → `terraform` workflow runs `fmt -check` + `validate` (all envs,
   no creds) + the static policy tests.
2. `workflow_dispatch` with `environment: dev` → `terraform plan` (OIDC to Azure,
   no stored secret). Review the plan, then `apply`.
3. Repeat for `test`, then `prod`. `prod` is protected by a GitHub **Environment**
   rule (required reviewers), so promotion to prod needs human approval.
4. `apply` is intentionally a separate, manually-approved step — never auto-applied.

### Local commands

```bash
# format + validate (no cloud creds; validate needs no remote state)
terraform -chdir=infra/terraform fmt -recursive
terraform -chdir=infra/terraform/envs/dev init -backend=false && \
  terraform -chdir=infra/terraform/envs/dev validate

# plan a real environment (needs Azure + Databricks auth + backend config)
terraform -chdir=infra/terraform/envs/dev  init -backend-config=backend.hcl
terraform -chdir=infra/terraform/envs/dev  plan
terraform -chdir=infra/terraform/envs/test plan
terraform -chdir=infra/terraform/envs/prod plan   # gated: prod approval required

# regenerate policy-derived grants/schemas after editing governance/policy.py
python pipelines/generate_terraform_grants.py
```

## Invariants the config holds

The Terraform + generated grants are structured so that:

- required variables are **declared** and **supplied** in every env,
- environment naming is consistent (dir == `environment` == tag == `investsphere_<env>` == state key),
- generated grants give **no** PII/raw-schema path to engineers/analysts,
- all required UC schemas are provisioned, and envs load them from `generated/`,
- cost/governance tags (`project`, `environment`, `owner`, `cost_center`) are present and validated,
- generated files match the governance policy model (drift guard),
- containers, the 5 groups, the 5 secret categories, ADLS Gen2 + TLS1.2, cluster
  auto-termination + cost tags, and warehouse auto-stop are all present.

The generated-grants drift check (`git diff --exit-code`) in the `terraform`
workflow fails the build if the committed files ever diverge from the policy model.
