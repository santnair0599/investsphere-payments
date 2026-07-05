# Account-level Unity Catalog groups. Requires the account-scoped Databricks
# provider (host = https://accounts.azuredatabricks.net, account_id set).
# Membership (Entra ID users / SPNs -> group) is typically synced via SCIM;
# databricks_group_member resources can be added per environment as needed.

resource "databricks_group" "groups" {
  for_each     = toset(var.groups)
  display_name = each.value
}
