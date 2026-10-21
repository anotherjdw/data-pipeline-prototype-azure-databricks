terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "4.49.0"
    }
  }
}

locals {
  project_environment = "${var.project}-${var.environment}"

  common_tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    module      = "workspace"
  }
}

resource "azurerm_resource_group" "prototype_resource_group" {
  name     = "${local.project_environment}-rg"
  location = var.location
  tags     = local.common_tags
}

resource "azurerm_databricks_workspace" "prototype_databricks_workspace" {
  name                = "${local.project_environment}-databricks-workspace"
  resource_group_name = azurerm_resource_group.prototype_resource_group.name
  location            = var.location
  sku                 = "premium"
  tags                = local.common_tags
}