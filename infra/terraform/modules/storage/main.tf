terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "4.49.0"
    }
  }
}

locals {
  common_tags = {
    project     = var.project
    environment = var.environment
    managed_by  = "terraform"
    module      = "storage"
  }
}

resource "azurerm_storage_account" "medallion_storage" {
  name                     = "dppmedallionstorage${var.environment}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = local.common_tags
}

resource "azurerm_storage_container" "medallion_containers" {
  for_each = var.container_names

  name               = each.value
  storage_account_id = azurerm_storage_account.medallion_storage.id
}