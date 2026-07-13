# Dev uses LOCAL state to avoid the remote-state chicken-and-egg while you're
# building solo. Switch back to the azurerm backend below once you have a state
# storage account (init -backend-config=backend.hcl migrates the state).
terraform {
  backend "local" {}
}

# Remote state in Azure (re-enable for CI / shared use):
#   terraform init -backend-config=backend.hcl
# terraform {
#   backend "azurerm" {
#     key = "investsphere-payments/dev.tfstate"
#   }
# }
