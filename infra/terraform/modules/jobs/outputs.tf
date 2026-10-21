output "job_names" {
  description = "Map of job key to the full Databricks job name."
  value       = { for k, v in databricks_job.etl_jobs : k => v.name }
}

output "job_ids" {
  description = "Map of job key to the Databricks job ID, e.g. for databricks jobs run-now."
  value       = { for k, v in databricks_job.etl_jobs : k => v.id }
}
