variable "catalog_name" {
  type        = string
  description = "Unity Catalog catalog name (e.g. investsphere_dev)."
}

variable "schemas" {
  type        = list(string)
  description = "Schemas to create in the catalog (from generated/schemas.auto.tfvars.json)."
}

variable "storage_credential_name" {
  type        = string
  description = "UC storage credential name."
}

variable "access_connector_id" {
  type        = string
  description = "Azure Databricks Access Connector id backing the storage credential."
}

variable "external_location_name" {
  type        = string
  description = "UC external location name."
}

variable "external_location_url" {
  type        = string
  description = "abfss:// URL the external location points at."
}

variable "catalog_grants" {
  type        = map(list(string))
  description = "principal -> catalog privileges (from generated grants)."
}

variable "schema_grants" {
  type = list(object({
    principal  = string
    schema     = string
    privileges = list(string)
  }))
  description = "Per-schema USE grants (from generated grants, aligned to policy)."
}

variable "tags" {
  type        = map(string)
  description = "Tags applied as catalog properties."
}
