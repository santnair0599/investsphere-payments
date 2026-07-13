variable "subscription_id" { type = string }
variable "tenant_id" { type = string }
variable "databricks_host" { type = string }
variable "databricks_account_id" { type = string }

variable "location" { type = string }
variable "environment" {
  type = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be one of dev, test, prod."
  }
}

variable "resource_group_name" { type = string }
variable "storage_account_name" { type = string }
variable "key_vault_name" { type = string }
variable "access_connector_name" { type = string }
variable "catalog_name" { type = string }

variable "databricks_app_object_id" {
  type        = string
  description = "Object id of the AzureDatabricks first-party app (tenant-stable). Grants Key Vault Secrets User so KV-backed secret scopes read at runtime."
  default     = ""
}

variable "etl_service_principal" {
  type        = string
  description = "ETL service principal (job run-as in test/prod); set to the SPN application id in prod."
  default     = "spn_investsphere_etl"
}

variable "warehouse_size" {
  type    = string
  default = "Small"
}
variable "auto_stop_minutes" {
  type    = number
  default = 10
}
variable "serverless_enabled" {
  type    = bool
  default = true
}
variable "create_placeholder_values" {
  type    = bool
  default = false
}

variable "tags" {
  type = map(string)
  validation {
    condition = alltrue([
      for k in ["project", "environment", "owner", "cost_center"] : contains(keys(var.tags), k)
    ])
    error_message = "tags must include project, environment, owner, cost_center."
  }
}
