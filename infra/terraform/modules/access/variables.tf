variable "project" {
  description = "Name of the data pipeline project."
  type        = string
}

variable "environment" {
  description = "Defines the environment in which the resources are created."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group the Access Connector is created in."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "storage_account_id" {
  description = "ID of the storage account the Access Connector is given access to."
  type        = string
}

variable "container_urls" {
  description = "abfss:// URL of each container, keyed by medallion layer. One external location is created per container."
  type        = map(string)
}
