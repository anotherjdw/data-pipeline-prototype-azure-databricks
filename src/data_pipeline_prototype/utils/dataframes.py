import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .data_quality import check_no_duplicate_rows
from .python import validate_dates


def validate_pyspark_dataframe(
    data: DataFrame, expected_schema: t.StructType, unique_key: list[str]
) -> None:
    """Validates a DataFrame's schema and data before it is written.

    Runs three checks in order, stopping at the first that fails: the column names
    and types match `expected_schema` exactly, columns marked non-nullable contain no
    nulls, and no two rows share the same `unique_key` values.

    Args:
        data: The DataFrame to validate.
        expected_schema: The schema the DataFrame must match. A field's `nullable`
            flag decides whether that column may contain nulls.
        unique_key: Columns whose combined values must be unique across all rows.

    Raises:
        ValueError: If any check fails. A schema failure lists every missing column,
            unexpected column and type mismatch at once.
    """
    expected_column_names = set(expected_schema.fieldNames())
    expected_column_types = {field.name: field.dataType for field in expected_schema.fields}

    actual_column_names = set(data.columns)
    actual_column_types = {field.name: field.dataType for field in data.schema.fields}

    columns_missing_from_expected_schema = expected_column_names - actual_column_names
    unexpected_columns_in_actual_schema = actual_column_names - expected_column_names

    validation_errors = []

    # collect columns missing from expected schema
    if columns_missing_from_expected_schema:
        validation_errors.append(f"Missing columns: {columns_missing_from_expected_schema}")

    # collect unexpected columns coming from actual dataframe
    if unexpected_columns_in_actual_schema:
        validation_errors.append(f"Unexpected columns: {unexpected_columns_in_actual_schema}")

    # identify data type errors
    type_errors = []
    for col_name in expected_column_names & actual_column_names:
        expected_type = expected_column_types[col_name]
        actual_type = actual_column_types[col_name]
        if actual_type != expected_type:
            type_errors.append(
                f"'{col_name}': Expected {expected_type.simpleString()}, Received {actual_type.simpleString()}"
            )

    # collect data type errors
    if type_errors:
        validation_errors.append(f"Type mismatches: {', '.join(type_errors)}")

    if validation_errors:
        error_message = "Schema validation failed:\n- " + "\n- ".join(validation_errors)
        raise ValueError(error_message)

    non_nullable = [field.name for field in expected_schema.fields if not field.nullable]
    if non_nullable:
        null_counts = data.select(
            [f.count(f.when(f.col(c).isNull(), 1)).alias(c) for c in non_nullable]
        ).first()
        columns_with_nulls = {c: null_counts[c] for c in non_nullable if null_counts[c] > 0}
        if columns_with_nulls:
            raise ValueError(
                f"Data validation failed: nulls in non-nullable columns {columns_with_nulls}"
            )

    uniqueness = check_no_duplicate_rows(data, subset=unique_key)
    if not uniqueness.passed:
        raise ValueError(f"Data validation failed: {uniqueness.message}")


def read_raw_files(
    spark: SparkSession,
    file_path: str,
    schema: StructType | None = None,
    file_format: str = "json",
    date_ranges: list[str] | None = None,
    partition_key: str | None = None,
) -> DataFrame:
    """Reads ingested raw files from storage, optionally limited to some days.

    With `partition_key` and `date_ranges`, only the `{partition_key}={day}`
    directories under `file_path` are loaded, and `partition_key` is kept as a column
    through the `basePath` option. Without them, everything under `file_path` is read.

    Args:
        spark: Active SparkSession.
        file_path: Root directory of the raw dataset, e.g.
            abfss://raw@<account>.dfs.core.windows.net/orders/.
        schema: Schema to read the files with. If it leaves out `partition_key`,
            Spark appends that column with an inferred type.
        file_format: Format of the raw files, e.g. json.
        date_ranges: Days to read, as YYYY-MM-DD strings.
        partition_key: The Hive-style partition directory name, e.g. OrderDate.

    Returns:
        A DataFrame with the requested days' raw data.

    Raises:
        ValueError: If a value in `date_ranges` is not a YYYY-MM-DD date.
        FileNotFoundError: If at least one requested day has no directory. The whole
            read fails; only the first missing path is named in the message.
    """
    reader = spark.read.format(file_format).option("multiLine", "true")
    if schema is not None:
        reader = reader.schema(schema)

    if partition_key and date_ranges:
        validate_dates(date_ranges)
        base = file_path.rstrip("/")
        paths = [f"{base}/{partition_key}={day}" for day in date_ranges]
        try:
            return reader.option("basePath", base).load(paths)
        except AnalysisException as e:
            if e.getErrorClass() != "PATH_NOT_FOUND":
                raise
            raise FileNotFoundError(
                f"No raw data at {e.getMessageParameters()['path']}. Every day from "
                f"{min(date_ranges)} to {max(date_ranges)} must exist under {base}; "
                "if any day in the range is missing, the whole run fails. "
                "Rerun with a range that excludes the missing days."
            ) from e

    return reader.load(file_path)


def read_delta_table(
    spark: SparkSession,
    table_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
    partition_key: str | None = None,
) -> DataFrame:
    """Reads a Delta table, optionally filtered to a date range of its partition column.

    Unlike read_raw_files, days in the range that have no data are not an error: they
    simply contribute no rows.

    Args:
        spark: Active SparkSession.
        table_name: Table to read, e.g. preprocessed.orders, or
            <catalog>.<schema>.<table> in Unity Catalog.
        start_date: First day to read (inclusive), as YYYY-MM-DD.
        end_date: Last day to read (inclusive), as YYYY-MM-DD.
        partition_key: The table's date partition column, e.g. date_utc. Leave it
            unset, together with both dates, to read the whole table.

    Returns:
        A DataFrame with the table's rows from start_date to end_date, or all rows if
        no partition_key is given.

    Raises:
        ValueError: If dates are given without a partition_key, partition_key is
            given without both dates, a date is not YYYY-MM-DD, or start_date is
            after end_date.
    """
    if not partition_key:
        if start_date or end_date:
            raise ValueError("partition_key is required when start_date or end_date is given.")
        return spark.read.table(table_name)

    elif start_date and end_date:
        validate_dates([start_date, end_date])
        if start_date > end_date:
            raise ValueError(f"start_date {start_date} is after end_date {end_date}.")
        return spark.read.table(table_name).filter(
            f.col(partition_key).between(start_date, end_date)
        )

    else:
        raise ValueError(
            "start_date and end_date must be provided. Only leave both start_date and end_date null if the input table has no partition_key."
        )


def write_delta_table(output_dataframe: DataFrame, output_table: str) -> None:
    """Writes a DataFrame to an existing Delta table, replacing only the partitions it covers.

    Uses dynamic partition overwrite: each partition value present in
    `output_dataframe` is replaced, and every other partition is kept. Rerunning the
    same write gives the same result, and an empty DataFrame changes nothing. On a table
    without partitions, the whole table is replaced. Columns are matched by name, and
    Delta rejects data whose columns or types do not match the table.

    Uses writeTo().overwritePartitions() rather than saveAsTable(): with saveAsTable in
    overwrite mode, Delta replaces the whole table, whatever overwrite options are set.

    Args:
        output_dataframe: The validated data to write.
        output_table: Table to write to, e.g. preprocessed.orders. It must already
            exist; Terraform creates it in Databricks.

    Raises:
        AnalysisException: If the table does not exist, or the DataFrame's columns or
            types do not match it.
    """
    output_dataframe.writeTo(output_table).overwritePartitions()
