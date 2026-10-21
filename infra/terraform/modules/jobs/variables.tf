variable "project" {
  description = "Name of the data pipeline project."
  type        = string
}

variable "environment" {
  description = "Defines the environment in which the resources are created."
  type        = string
}

variable "jobs" {
  description = "ETL jobs to create, projected from each jobs/<job>/<job>.yaml on disk."
  type = map(object({
    cluster = object({
      node_type_id = string
      num_workers  = number
    })
    arguments = map(string)
  }))
}

variable "code_volume_path" {
  description = "Path of the Volume that holds the wheel and job entry scripts, e.g. /Volumes/artifacts/<schema>/code."
  type        = string
}

variable "wheel_file_name" {
  description = "File name of the data_pipeline_prototype wheel in the code Volume."
  type        = string
}

variable "spark_version" {
  description = "Databricks Runtime of the job clusters (replaces glue_version)."
  type        = string
  default     = "15.4.x-scala2.12" # 15.4 LTS: Spark 3.5, Python 3.11
}

variable "scheduled_triggers" {
  description = "Jobs started on a schedule: job name to a Quartz cron expression in UTC, e.g. 0 10 15 * * ? (15:10 daily)."
  type        = map(string)
  default     = {}
}

variable "schedules_paused" {
  description = "Create schedules paused, so jobs only run when started by hand until set to false."
  type        = bool
  default     = true
}
