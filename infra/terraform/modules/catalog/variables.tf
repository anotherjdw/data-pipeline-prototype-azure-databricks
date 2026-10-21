variable "project" {
  description = "Name of the data pipeline project."
  type        = string
}

variable "environment" {
  description = "Defines the environment in which the resources are created."
  type        = string
}

variable "tables" {
  description = "Tables to create, projected from each tables/<layer>/<dataset>/<table>.yaml on disk, keyed <layer>/<dataset>/<table>."
  type        = any
}

variable "storage_roots" {
  description = "abfss:// URL of each layer's external location, keyed by layer. Used as the catalogs' managed storage."
  type        = map(string)
}

variable "data_writers" {
  description = "Users (e-mail) or groups (name) that receive read and write access to the pipeline's catalog. The deploying identity owns the catalog automatically has access."
  type        = set(string)
  default     = []
}