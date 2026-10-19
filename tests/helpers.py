from pathlib import Path

import yaml
from pyspark.sql.types import DataType, StructType

TABLES_ROOT = Path(__file__).parents[1] / "src" / "data_pipeline_prototype" / "tables"


def _sql_string(value: str) -> str:
    """Quotes a value as a Spark SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def create_table_statement(layer: str, dataset: str, table: str) -> str:
    """Builds the CREATE TABLE statement for a table YAML, as a local stand-in for Terraform.

    Reads tables/<layer>/<dataset>/<table>.yaml and creates a partitioned Delta table named
    <dataset>.<table>, so tests write to a table with the same columns, types, nullability
    and partitioning as the one Terraform creates in Unity Catalog. In Databricks the layer
    is the catalog (<layer>.<dataset>.<table>); locally Spark has a single catalog, so the
    layer only locates the YAML file.

    Args:
        layer: Medallion layer, e.g. preprocessed.
        dataset: Dataset, used as the database (Unity Catalog schema) name, e.g.
            customer_tracking.
        table: Table name, e.g. orders.

    Returns:
        A CREATE TABLE ... USING delta statement.
    """
    spec = yaml.safe_load((TABLES_ROOT / layer / dataset / f"{table}.yaml").read_text())["table"]

    columns = [
        f"{c['name']} {c['type']}"
        f"{'' if c.get('nullable', True) else ' NOT NULL'}"
        f" COMMENT {_sql_string(c['comment'])}"
        for c in spec["columns"] + spec["partition_keys"]
    ]
    partition_keys = ", ".join(p["name"] for p in spec["partition_keys"])

    return (
        f"CREATE TABLE {dataset}.{table} (\n  "
        + ",\n  ".join(columns)
        + f"\n) USING delta PARTITIONED BY ({partition_keys})"
        + f" COMMENT {_sql_string(spec['description'])}"
    )


def column_types(schema: StructType) -> dict[str, DataType]:
    """Map each column name to its data type, ignoring nullable.

    Comparing these dicts is the schema check for DataFrames read back from files:
    Spark marks every column read from a file as nullable, so comparing StructFields
    directly fails whenever the expected schema has a non-nullable field.
    """
    return {field.name: field.dataType for field in schema.fields}
