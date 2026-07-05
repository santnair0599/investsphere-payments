# Outputs consumed by the Databricks Asset Bundle (see docs/DATABRICKS_DEPLOYMENT.md
# and bundle_vars/<env>.yml). The output NAMES are a contract with the bundle:
#   catalog_name          -> bundle var `catalog`
#   warehouse_id          -> bundle var `warehouse_id`
#   secret_scope_name     -> bundle var `secret_scope`
#   etl_service_principal -> bundle var `etl_service_principal`
# tests/test_bundle.py asserts these names line up with databricks.yml variables.

output "catalog_name" {
  description = "Unity Catalog catalog for this environment -> bundle var `catalog`."
  value       = module.unity_catalog.catalog_name
}

output "schema_names" {
  description = "Schemas created in the catalog (bundle deploys into these)."
  value       = module.unity_catalog.schema_names
}

output "warehouse_id" {
  description = "SQL warehouse id for dbt/BI -> bundle var `warehouse_id`."
  value       = module.compute.warehouse_id
}

output "cluster_policy_id" {
  description = "Job cluster policy id (cost guardrails)."
  value       = module.compute.cluster_policy_id
}

output "secret_scope_name" {
  description = "Key Vault-backed Databricks secret scope -> bundle var `secret_scope`."
  value       = module.secrets.secret_scope_name
}

output "etl_service_principal" {
  description = "ETL service principal (job run-as in test/prod) -> bundle var `etl_service_principal`."
  value       = var.etl_service_principal
}

output "external_location_url" {
  description = "abfss:// lake root the catalog is rooted at (storage path)."
  value       = local.external_location_url
}

output "storage_account_name" {
  description = "ADLS Gen2 storage account backing the lake."
  value       = module.azure_foundation.storage_account_name
}

output "access_connector_id" {
  description = "Databricks access connector / managed identity id."
  value       = module.azure_foundation.access_connector_id
}

output "resource_group_name" {
  description = "Resource group holding the platform foundation."
  value       = module.azure_foundation.resource_group_name
}
