# Remote state in Azure. Supply the storage details per env via -backend-config
# (CI) or a backend.hcl so they aren't committed:
#   terraform init -backend-config=backend.hcl
terraform {
  backend "azurerm" {
    key = "investsphere-payments/dev.tfstate"
  }
}
