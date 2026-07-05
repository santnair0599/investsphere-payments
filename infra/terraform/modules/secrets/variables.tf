variable "secret_scope_name" {
  type        = string
  description = "Databricks secret scope name (Key Vault-backed)."
}

variable "key_vault_id" {
  type        = string
  description = "Azure Key Vault resource id backing the scope."
}

variable "key_vault_uri" {
  type        = string
  description = "Azure Key Vault DNS URI (https://<kv>.vault.azure.net/)."
}

variable "secret_names" {
  type        = list(string)
  description = "Secret keys to provision as placeholders in Key Vault."
  default = [
    "oracle-jdbc-user", "oracle-jdbc-password",
    "sqlserver-jdbc-user", "sqlserver-jdbc-password",
    "salesforce-client-id", "salesforce-client-secret",
    "sftp-user", "sftp-password",
    "rest-api-token",
  ]
}

variable "create_placeholder_values" {
  type        = bool
  description = "Create placeholder secret VALUES (false in prod — set out of band)."
  default     = false
}
