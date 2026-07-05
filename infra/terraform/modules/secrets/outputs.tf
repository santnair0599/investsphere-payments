output "secret_scope_name" {
  value = databricks_secret_scope.kv.name
}

output "provisioned_secret_keys" {
  value = var.secret_names
}
