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
