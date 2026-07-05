# The secret scope is a databricks/databricks resource; the placeholder secret
# values are azurerm Key Vault secrets — so this module needs both providers.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
  }
}
