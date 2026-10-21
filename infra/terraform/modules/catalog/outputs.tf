output "table_names" {
  description = "Full Unity Catalog name (<catalog>.<schema>.<table>) of each table, keyed <layer>/<dataset>/<table>."
  value       = { for k, t in databricks_sql_table.tables : k => "${t.catalog_name}.${t.schema_name}.${t.name}" }
}

output "code_volume_path" {
  description = "Path of the Volume that holds the wheel and job entry scripts."
  value       = "/Volumes/${databricks_volume.code.catalog_name}/${databricks_volume.code.schema_name}/${databricks_volume.code.name}"
}

output "warehouse_id" {
  description = "ID of the SQL warehouse that creates the tables, also usable for queries."
  value       = databricks_sql_endpoint.ddl.id
}
