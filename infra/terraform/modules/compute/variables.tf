variable "cluster_policy_name" {
  type        = string
  description = "Job cluster policy name."
}

variable "warehouse_name" {
  type        = string
  description = "SQL warehouse name for dbt / BI."
}

variable "warehouse_size" {
  type        = string
  description = "SQL warehouse cluster size."
  default     = "Small"
}

variable "auto_stop_minutes" {
  type        = number
  description = "Idle minutes before the warehouse / clusters auto-terminate."
  default     = 10
}

variable "serverless_enabled" {
  type        = bool
  description = "Use serverless SQL warehouse where available."
  default     = true
}

variable "max_dbu_per_hour" {
  type        = number
  description = "Cost guardrail: max autoscale DBU/hour the policy allows."
  default     = 40
}

variable "tags" {
  type        = map(string)
  description = "Cost/governance tags applied as custom_tags."
}
