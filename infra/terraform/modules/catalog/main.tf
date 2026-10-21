terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.136.0"
    }
  }
}

locals {
  # One catalog per medallion layer that has tables, e.g. preprocessed.
  layers = toset([for t in values(var.tables) : t.layer])

  # One schema per layer/dataset pair, even if a dataset holds several tables.
  schemas = {
    for key in distinct([for t in values(var.tables) : "${t.layer}/${t.dataset}"]) :
    key => { layer = split("/", key)[0], dataset = split("/", key)[1] }
  }
}

# --- Medallion catalogs, schemas and tables, projected from tables/<layer>/<dataset>/<table>.yaml ---

resource "databricks_catalog" "layers" {
  for_each = local.layers

  name         = each.key
  storage_root = var.storage_roots[each.key] # managed tables store their files in the layer's container
  comment      = "${title(each.key)} layer of ${var.project}. Managed by Terraform."
}

resource "databricks_schema" "datasets" {
  for_each = local.schemas

  catalog_name = databricks_catalog.layers[each.value.layer].name
  name         = each.value.dataset
  comment      = "Tables of the ${each.value.dataset} dataset in the ${each.value.layer} layer."
}

# databricks_sql_table runs CREATE/ALTER TABLE on Databricks, so it needs compute to run on.
# A small serverless SQL warehouse starts in seconds and stops itself when idle; it can also
# be used to query the tables from the Databricks UI.
resource "databricks_sql_endpoint" "ddl" {
  name                      = "${var.project}-${var.environment}-warehouse"
  cluster_size              = "2X-Small"
  max_num_clusters          = 1
  auto_stop_mins            = 10
  enable_serverless_compute = true

  tags {
    custom_tags {
      key   = "project"
      value = var.project
    }
    custom_tags {
      key   = "environment"
      value = var.environment
    }
  }
}

resource "databricks_sql_table" "tables" {
  for_each = var.tables

  catalog_name       = databricks_catalog.layers[each.value.layer].name
  schema_name        = databricks_schema.datasets["${each.value.layer}/${each.value.dataset}"].name
  name               = each.value.table
  table_type         = "MANAGED"
  data_source_format = "DELTA"
  comment            = each.value.description
  partitions         = [for p in each.value.partition_keys : p.name]
  warehouse_id       = databricks_sql_endpoint.ddl.id

  # Unlike a Glue table, the partition columns are part of the column list.
  dynamic "column" {
    for_each = concat(each.value.columns, each.value.partition_keys)
    content {
      name     = column.value.name
      type     = column.value.type
      nullable = try(column.value.nullable, true) # nullable: false in the YAML becomes NOT NULL
      comment  = column.value.comment
    }
  }
}

# --- Job code: the wheel and job entry scripts the workflow uploads (replaces the S3 scripts bucket) ---

resource "databricks_catalog" "artifacts" {
  name         = "artifacts"
  storage_root = var.storage_roots["artifacts"]
  comment      = "Deployment artifacts of ${var.project}. Managed by Terraform."
}

resource "databricks_schema" "project" {
  catalog_name = databricks_catalog.artifacts.name
  name         = replace(var.project, "-", "_")
  comment      = "Deployment artifacts of ${var.project}."
}

resource "databricks_volume" "code" {
  catalog_name = databricks_catalog.artifacts.name
  schema_name  = databricks_schema.project.name
  name         = "code"
  volume_type  = "MANAGED"
  comment      = "Python wheel and job entry scripts, uploaded by the deploy workflow."
}
