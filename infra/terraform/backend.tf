terraform {
  backend "azurerm" {
    resource_group_name  = "data-pipeline-prototype-prototype-tfstate"
    storage_account_name = "prototypetfstate"
    container_name       = "tfstate"
    key                  = "global/terraform.tfstate"
    use_azuread_auth     = true
  }
}