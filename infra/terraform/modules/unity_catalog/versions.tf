# Catalog, schemas, grants, external location and storage credential are all
# databricks/databricks resources.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}
