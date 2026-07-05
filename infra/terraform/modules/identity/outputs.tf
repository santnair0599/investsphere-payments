output "group_names" {
  value = [for g in databricks_group.groups : g.display_name]
}
