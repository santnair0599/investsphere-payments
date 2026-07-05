# Compute: a job cluster policy (cost controls + auto-termination + mandatory
# tags) and a SQL warehouse for dbt Gold / BI.

resource "databricks_cluster_policy" "jobs" {
  name = var.cluster_policy_name
  definition = jsonencode({
    "autotermination_minutes" : {
      "type" : "fixed", "value" : var.auto_stop_minutes
    },
    "spark_version" : {
      "type" : "unlimited", "defaultValue" : "auto:latest-lts"
    },
    # cost guardrail: cap autoscale so jobs can't request huge clusters
    "autoscale.max_workers" : {
      "type" : "range", "maxValue" : 8, "defaultValue" : 4
    },
    "dbus_per_hour" : {
      "type" : "range", "maxValue" : var.max_dbu_per_hour
    },
    # mandatory cost-attribution tags
    "custom_tags.project" : { "type" : "fixed", "value" : var.tags["project"] },
    "custom_tags.environment" : { "type" : "fixed", "value" : var.tags["environment"] },
    "custom_tags.cost_center" : { "type" : "fixed", "value" : var.tags["cost_center"] }
  })
}

resource "databricks_sql_endpoint" "warehouse" {
  name                      = var.warehouse_name
  cluster_size              = var.warehouse_size
  auto_stop_mins            = var.auto_stop_minutes
  enable_serverless_compute = var.serverless_enabled
  warehouse_type            = "PRO"

  tags {
    dynamic "custom_tags" {
      for_each = var.tags
      content {
        key   = custom_tags.key
        value = custom_tags.value
      }
    }
  }
}
