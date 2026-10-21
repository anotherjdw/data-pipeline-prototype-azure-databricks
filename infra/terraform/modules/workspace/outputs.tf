output "resource_group_name" {
  description = "Resource group the storage and access modules create their resources in."
  value       = azurerm_resource_group.prototype_resource_group.name
}

output "location" {
  description = "Region of the resource group, used by the storage and access modules."
  value       = azurerm_resource_group.prototype_resource_group.location
}

output "workspace_url" {
  description = "Workspace host name without https://, for the databricks provider."
  value       = azurerm_databricks_workspace.prototype_databricks_workspace.workspace_url
}

output "workspace_resource_id" {
  description = "Azure resource ID of the workspace, which the databricks provider signs in with."
  value       = azurerm_databricks_workspace.prototype_databricks_workspace.id
}

