# Unity Catalog: storage credential (via the access connector), external
# location, the per-environment catalog, its schemas, and catalog/schema grants.
#
# Ownership note: catalog + schema + USE grants are owned HERE (Terraform).
# Table/column-mask/row-filter/view grants are owned by the governance SQL
# (governance/sql/, generated from the same policy model). See docs/TERRAFORM.md.

resource "databricks_storage_credential" "this" {
  name = var.storage_credential_name
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
  comment = "Managed-identity credential for ${var.catalog_name}"
}

resource "databricks_external_location" "this" {
  name            = var.external_location_name
  url             = var.external_location_url
  credential_name = databricks_storage_credential.this.name
  comment         = "Lake root for ${var.catalog_name}"
}

resource "databricks_catalog" "this" {
  name         = var.catalog_name
  storage_root = var.external_location_url
  comment      = "InvestSphere Payments catalog (${var.catalog_name})"
  properties   = var.tags
  depends_on   = [databricks_external_location.this]
}

resource "databricks_schema" "schemas" {
  for_each     = toset(var.schemas)
  catalog_name = databricks_catalog.this.name
  name         = each.value
  comment      = "Managed schema ${each.value}"
  properties   = var.tags
}

# ---- catalog-level grants (USE CATALOG) ----
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name
  dynamic "grant" {
    for_each = var.catalog_grants
    content {
      principal  = grant.key
      privileges = grant.value
    }
  }
}

# ---- schema-level grants (USE SCHEMA), least-privilege per policy ----
# group by schema so each schema gets one databricks_grants resource.
locals {
  grants_by_schema = {
    for s in var.schemas : s => [
      for g in var.schema_grants : {
        principal  = g.principal
        privileges = g.privileges
      } if g.schema == s
    ]
  }
}

resource "databricks_grants" "schemas" {
  for_each = { for s, gs in local.grants_by_schema : s => gs if length(gs) > 0 }
  schema   = "${databricks_catalog.this.name}.${each.key}"
  dynamic "grant" {
    for_each = each.value
    content {
      principal  = grant.value.principal
      privileges = grant.value.privileges
    }
  }
  depends_on = [databricks_schema.schemas]
}
