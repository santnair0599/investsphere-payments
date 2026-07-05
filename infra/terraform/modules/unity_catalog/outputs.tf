output "catalog_name" {
  value = databricks_catalog.this.name
}

output "schema_names" {
  value = [for s in databricks_schema.schemas : s.name]
}

output "external_location_name" {
  value = databricks_external_location.this.name
}

output "storage_credential_name" {
  value = databricks_storage_credential.this.name
}
