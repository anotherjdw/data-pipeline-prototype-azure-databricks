output "workspace_url" {
  description = "URL of the Databricks workspace."
  value       = "https://${module.workspace.workspace_url}"
}

output "storage_account_name" {
  description = "Name of the medallion storage account."
  value       = module.storage.storage_account_name
}

output "table_names" {
  description = "Full Unity Catalog name of each table, keyed <layer>/<dataset>/<table>."
  value       = module.catalog.table_names
}

output "code_volume_path" {
  description = "Volume the deploy workflow uploads the wheel and job entry scripts to (replaces scripts_bucket)."
  value       = module.catalog.code_volume_path
}

output "job_names" {
  description = "Map of job key to the full Databricks job name (replaces glue_job_names)."
  value       = module.jobs.job_names
}

output "job_ids" {
  description = "Map of job key to the Databricks job ID."
  value       = module.jobs.job_ids
}
