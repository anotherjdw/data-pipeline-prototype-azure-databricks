output "storage_account_id" {
  description = "ID of the storage account, for the access module's role assignment."
  value       = azurerm_storage_account.medallion_storage.id
}

output "storage_account_name" {
  description = "Name of the storage account."
  value       = azurerm_storage_account.medallion_storage.name
}

output "container_urls" {
  # Ends with "/" because Databricks stores external location URLs with one; without it the
  # catalogs' storage_root changes during apply and Terraform stops with an inconsistent plan.
  description = "abfss:// URL of each container, keyed by medallion layer, ending in /."
  value       = { for k, v in azurerm_storage_container.medallion_containers : k => "abfss://${v.name}@${azurerm_storage_account.medallion_storage.primary_dfs_host}/" }
}
