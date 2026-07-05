# Declare databricks/databricks so the account-scoped provider passed in from the
# root (providers = { databricks = databricks.account }) is matched correctly and
# Terraform does not assume the hashicorp/databricks placeholder.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}
