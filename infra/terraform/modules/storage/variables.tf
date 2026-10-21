variable "project" {
  description = "Project Name"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}

variable "resource_group_name" {
  description = "Resource group the storage account is created in."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "container_names" {
  description = "Map of medallion layer to storage container name. Set in the root terraform.tfvars."
  type        = map(string)
}