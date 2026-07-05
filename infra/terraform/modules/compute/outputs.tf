output "cluster_policy_id" {
  value = databricks_cluster_policy.jobs.id
}

output "warehouse_id" {
  value = databricks_sql_endpoint.warehouse.id
}
