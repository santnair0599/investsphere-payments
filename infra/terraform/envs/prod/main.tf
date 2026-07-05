# Root composition for an environment. Schema list + grants are loaded from the
# generated files (produced from the governance policy model), so infra grants
# stay aligned with the security policy the tests enforce.

locals {
  schemas = jsondecode(file("${path.module}/../../generated/schemas.auto.tfvars.json")).schemas
  grants  = jsondecode(file("${path.module}/../../generated/grants.auto.tfvars.json"))

  # gold container is the catalog storage root; medallion paths live beneath it.
  external_location_url = "abfss://gold@${var.storage_account_name}.dfs.core.windows.net/"
}

module "azure_foundation" {
  source                = "../../modules/azure_foundation"
  resource_group_name   = var.resource_group_name
  location              = var.location
  storage_account_name  = var.storage_account_name
  key_vault_name        = var.key_vault_name
  access_connector_name = var.access_connector_name
  tenant_id             = var.tenant_id
  tags                  = var.tags
}

module "identity" {
  source    = "../../modules/identity"
  providers = { databricks = databricks.account }
}

module "unity_catalog" {
  source                  = "../../modules/unity_catalog"
  catalog_name            = var.catalog_name
  schemas                 = local.schemas
  storage_credential_name = "${var.catalog_name}_cred"
  access_connector_id     = module.azure_foundation.access_connector_id
  external_location_name  = "${var.catalog_name}_root"
  external_location_url   = local.external_location_url
  catalog_grants          = local.grants.catalog_grants
  schema_grants           = local.grants.schema_grants
  tags                    = var.tags
  depends_on              = [module.identity]
}

module "compute" {
  source              = "../../modules/compute"
  cluster_policy_name = "investsphere_payments_${var.environment}_jobs"
  warehouse_name      = "investsphere_payments_${var.environment}_wh"
  warehouse_size      = var.warehouse_size
  auto_stop_minutes   = var.auto_stop_minutes
  serverless_enabled  = var.serverless_enabled
  tags                = var.tags
}

module "secrets" {
  source                    = "../../modules/secrets"
  secret_scope_name         = "investsphere_payments"
  key_vault_id              = module.azure_foundation.key_vault_id
  key_vault_uri             = "https://${var.key_vault_name}.vault.azure.net/"
  create_placeholder_values = var.create_placeholder_values
}
