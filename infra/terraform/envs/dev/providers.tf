terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}

# Workspace-scoped provider (catalogs, schemas, grants, compute, secrets).
provider "databricks" {
  host = var.databricks_host
}

# Account-scoped provider (account-level groups).
# Uses an OAuth U2M profile created via:
#   databricks auth login --host https://accounts.azuredatabricks.net \
#     --account-id <id> --profile investsphere-account
# (Azure-CLI passthrough doesn't work for the account API on a personal-MSA tenant.)
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
  profile    = "investsphere-account"
}
