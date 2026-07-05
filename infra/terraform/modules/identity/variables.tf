variable "groups" {
  type        = list(string)
  description = "Account-level groups to ensure exist."
  default = [
    "data_engineers",
    "analysts",
    "pii_approved_users",
    "data_stewards",
    "spn_investsphere_etl",
  ]
}
