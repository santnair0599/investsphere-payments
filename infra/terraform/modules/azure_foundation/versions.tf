# Declare the provider source so the root module's provider (azurerm) is matched
# to this module's resources (not the default hashicorp/azurerm placeholder).
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.110"
    }
  }
}
