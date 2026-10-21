terraform {
  required_version = "~> 1.10.0"

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

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# Signs in to the workspace with the same Azure identity as azurerm (az login locally,
# the GitHub OIDC service principal in CI).
provider "databricks" {
  host                        = "https://${module.workspace.workspace_url}"
  azure_workspace_resource_id = module.workspace.workspace_resource_id
}

locals {
  jobs_root = "${path.module}/../../src/data_pipeline_prototype/jobs"

  job_specs = {
    for spec in fileset(local.jobs_root, "*/*.yaml") :
    dirname(spec) => yamldecode(file("${local.jobs_root}/${spec}")).job
  }
  # Paths are built from each spec's layer + dataset, and table names from layer + dataset + table,
  # so storage account and catalog names come from the modules, not from the YAML file.
  jobs = {
    for name, job in local.job_specs : name => {
      cluster = job.cluster
      arguments = merge(
        job.arguments,
        { for arg, p in try(job.paths, {}) : arg => "${module.storage.container_urls[p.layer]}${p.dataset}/" },
        { for arg, t in try(job.tables, {}) : arg => module.catalog.table_names["${t.layer}/${t.dataset}/${t.table}"] }
      )
    }
  }

  tables_root = "${path.module}/../../src/data_pipeline_prototype/tables"
  # Names one catalog table per yaml file. Layer, dataset and table names come from the path, not the file.
  tables = {
    for spec in fileset(local.tables_root, "*/*/*.yaml") : trimsuffix(spec, ".yaml") => merge(
      yamldecode(file("${local.tables_root}/${spec}")).table,
      {
        layer   = split("/", spec)[0]
        dataset = split("/", spec)[1]
        table   = trimsuffix(basename(spec), ".yaml")
      }
    )
  }
}

module "workspace" {
  source      = "./modules/workspace"
  project     = var.project
  environment = var.environment
  location    = var.location
}

module "storage" {
  source              = "./modules/storage"
  project             = var.project
  environment         = var.environment
  resource_group_name = module.workspace.resource_group_name
  location            = module.workspace.location
  container_names     = var.container_names
}

module "access" {
  source              = "./modules/access"
  project             = var.project
  environment         = var.environment
  resource_group_name = module.workspace.resource_group_name
  location            = module.workspace.location
  storage_account_id  = module.storage.storage_account_id
  container_urls      = module.storage.container_urls
}

module "catalog" {
  source      = "./modules/catalog"
  project     = var.project
  environment = var.environment
  tables      = local.tables
  # The external locations' URLs, so the catalogs are created after Unity Catalog may use the containers.
  storage_roots = module.access.external_location_urls
}

# The identity Terraform signs in to Databricks with (the GitHub OIDC service principal in CI).
# Jobs have no run_as, so they run as this identity.
data "databricks_current_user" "deployer" {}

module "jobs" {
  source           = "./modules/jobs"
  project          = var.project
  environment      = var.environment
  jobs             = local.jobs
  single_user_name = data.databricks_current_user.deployer.user_name
  code_volume_path = module.catalog.code_volume_path # replaces scripts_bucket_name
  wheel_file_name  = var.wheel_file_name
  scheduled_triggers = {
    format_orders = "0 10 15 * * ?" # Quartz syntax, UTC: 15:10 daily
  }
  schedules_paused = var.schedules_paused
}
