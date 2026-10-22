variable "subscription_id" {
  description = "Azure subscription the resources are created in. Set TF_VAR_subscription_id rather than committing it."
  type        = string
}

variable "project" {
  description = "Name of the data pipeline project."
  type        = string
}

variable "environment" {
  description = "Defines the environment in which the resources are created."
  type        = string
}

variable "location" {
  description = "Azure region that hosts the resources."
  type        = string
  default     = "germanywestcentral"
}

variable "container_names" {
  description = "Map of medallion layer to storage container name."
  type        = map(string)
}

variable "wheel_file_name" {
  description = "File name of the data_pipeline_prototype wheel the deploy workflow uploads to the code Volume."
  type        = string
}

variable "schedules_paused" {
  description = "Create job schedules paused, so jobs only run when started by hand until set to false."
  type        = bool
  default     = true
}

variable "data_writers" {
  description = "Users (e-mail) or groups (name) that receive read and write access to the pipeline's catalog. The deploying identity owns the catalog automatically has access."
  type        = set(string)
  default     = []
}

variable "notification_emails" {
  description = "E-mail addresses notified when a job fails or times out."
  type        = list(string)
  default     = []
}