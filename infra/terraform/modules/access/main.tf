terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "4.49.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "1.136.0"
    }
  }
}

locals {
  project_environment = "${var.project}-${var.environment}"
  # Unity Catalog object names: underscores instead of hyphens.
  uc_prefix = replace(local.project_environment, "-", "_")

  common_tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    module      = "access"
  }
}

# 1. Access Connector: the Azure managed identity Unity Catalog uses to reach the storage account.
#    Replaces the Glue IAM role: the identity is the "who", the role assignment below the "what".
resource "azurerm_databricks_access_connector" "unity_catalog" {
  name                = "${local.project_environment}-access-connector"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = local.common_tags

  identity {
    type = "SystemAssigned"
  }
}

# 2. Let the Access Connector read and write data in the storage account's containers.
resource "azurerm_role_assignment" "unity_catalog_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
}

# 3. Let the Access Connector's managed identity read the Access Connector resource itself.
#    Unity Catalog checks for this Reader role when the storage credential is registered.
resource "azurerm_role_assignment" "unity_catalog_self_reader" {
  scope                = azurerm_databricks_access_connector.unity_catalog.id
  role_definition_name = "Reader"
  principal_id         = azurerm_databricks_access_connector.unity_catalog.identity[0].principal_id
}

# 4. Register the Access Connector in Unity Catalog as a storage credential.
resource "databricks_storage_credential" "unity_catalog" {
  name    = "${local.uc_prefix}_storage"
  comment = "Access Connector of ${local.project_environment}. Managed by Terraform."

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.unity_catalog.id
  }

  # Both roles must exist before Unity Catalog registers and uses the credential.
  depends_on = [
    azurerm_role_assignment.unity_catalog_storage,
    azurerm_role_assignment.unity_catalog_self_reader,
  ]
}

# 5. One external location per container: Unity Catalog only reads or writes abfss:// paths
#    that an external location covers. raw is read as files by the jobs; the others are the
#    managed storage of the catalogs created in the catalog module.
resource "databricks_external_location" "containers" {
  for_each = var.container_urls

  name            = "${local.uc_prefix}_${each.key}"
  url             = each.value
  credential_name = databricks_storage_credential.unity_catalog.name
  comment         = "${each.key} container of ${local.project_environment}. Managed by Terraform."
}
