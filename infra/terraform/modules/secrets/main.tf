# Secrets: a Key Vault-backed Databricks secret scope, plus placeholder Key Vault
# secrets for every source credential the ingestors read via
# investsphere_platform.config.secrets.get_secret(scope, key).
#
# Real values are set OUT OF BAND (CI secret, az keyvault secret set), never in
# Terraform state — placeholders are opt-in (create_placeholder_values) for
# lower environments only.

resource "databricks_secret_scope" "kv" {
  name = var.secret_scope_name
  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }
}

resource "azurerm_key_vault_secret" "placeholders" {
  for_each     = var.create_placeholder_values ? toset(var.secret_names) : toset([])
  name         = each.value
  value        = "REPLACE_ME"
  key_vault_id = var.key_vault_id
}
