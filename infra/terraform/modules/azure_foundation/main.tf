# Azure foundation: resource group, ADLS Gen2, medallion containers, Key Vault,
# and the Databricks Access Connector (managed identity) used by Unity Catalog to
# reach storage — so no per-user storage keys are ever needed.

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  is_hns_enabled           = true # ADLS Gen2 (hierarchical namespace)
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

# medallion filesystems: raw, bronze, silver, gold, quarantine, checkpoints
resource "azurerm_storage_data_lake_gen2_filesystem" "containers" {
  for_each           = toset(var.containers)
  name               = each.value
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_key_vault" "this" {
  name                       = var.key_vault_name
  resource_group_name        = azurerm_resource_group.this.name
  location                   = azurerm_resource_group.this.location
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
  enable_rbac_authorization  = true
  tags                       = var.tags
}

# Databricks Access Connector with a system-assigned managed identity.
resource "azurerm_databricks_access_connector" "this" {
  name                = var.access_connector_name
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  identity {
    type = "SystemAssigned"
  }
  tags = var.tags
}

# Let the connector's identity read/write the lake (UC storage access path).
resource "azurerm_role_assignment" "connector_storage" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

# The AzureDatabricks first-party app must be able to READ the vault, or
# Key Vault-backed secret scopes fail at runtime with PERMISSION_DENIED /
# ForbiddenByRbac (the vault uses RBAC authorization, so a data-plane role
# assignment is required — access policies don't apply).
resource "azurerm_role_assignment" "databricks_kv_secrets" {
  count                = var.databricks_app_object_id == "" ? 0 : 1
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.databricks_app_object_id
}
