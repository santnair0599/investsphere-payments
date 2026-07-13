variable "resource_group_name" {
  type        = string
  description = "Resource group for the platform foundation."
}

variable "location" {
  type        = string
  description = "Azure region."
}

variable "storage_account_name" {
  type        = string
  description = "ADLS Gen2 storage account name (globally unique, 3-24 lowercase)."
}

variable "key_vault_name" {
  type        = string
  description = "Azure Key Vault name."
}

variable "access_connector_name" {
  type        = string
  description = "Databricks Access Connector (managed identity) name."
}

variable "containers" {
  type        = list(string)
  description = "ADLS Gen2 filesystems / medallion paths to create."
  default     = ["raw", "bronze", "silver", "gold", "quarantine", "checkpoints"]
}

variable "tenant_id" {
  type        = string
  description = "Azure AD tenant id (for Key Vault)."
}

variable "tags" {
  type        = map(string)
  description = "Cost/governance tags: project, environment, owner, cost_center."
}

variable "databricks_app_object_id" {
  type        = string
  description = <<-EOT
    Object id of the AzureDatabricks first-party enterprise app in THIS tenant.
    Get it with:  az ad sp show --id 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query id -o tsv
    Granted `Key Vault Secrets User` on the vault so Key Vault-backed Databricks
    secret scopes resolve at RUNTIME (RBAC-mode vault). Empty string = skip.
  EOT
  default     = ""
}
