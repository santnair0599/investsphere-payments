# Remote state in Azure. Supply storage details via -backend-config (CI).
terraform {
  backend "azurerm" {
    key = "investsphere-payments/test.tfstate"
  }
}
