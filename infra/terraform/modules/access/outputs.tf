output "external_location_urls" {
  description = "abfss:// URL of each external location, keyed by layer. Referencing these (instead of the storage module's URLs) makes everything stored there wait until the location exists."
  value       = { for k, v in databricks_external_location.containers : k => v.url }
}

output "storage_credential_name" {
  description = "Name of the Unity Catalog storage credential backed by the Access Connector."
  value       = databricks_storage_credential.unity_catalog.name
}
