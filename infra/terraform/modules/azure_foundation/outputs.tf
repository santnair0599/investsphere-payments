output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = azurerm_storage_account.adls.name
}

output "storage_account_id" {
  value = azurerm_storage_account.adls.id
}

output "key_vault_id" {
  value = azurerm_key_vault.this.id
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}

output "container_names" {
  value = [for c in azurerm_storage_data_lake_gen2_filesystem.containers : c.name]
}
