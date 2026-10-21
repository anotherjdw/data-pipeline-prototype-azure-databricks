variable "project" {
  description = "Project Name"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "location" {
  description = "Azure region. Set in the root variables.tf/terraform.tfvars."
  type        = string
}