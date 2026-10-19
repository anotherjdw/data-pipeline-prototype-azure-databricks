import argparse
import re

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

import data_pipeline_prototype.utils.format_orders_constants as c
from data_pipeline_prototype.utils.data_quality import (
    assert_checks_pass,
    check_no_nulls,
    check_required_columns,
)
from data_pipeline_prototype.utils.dataframes import (
    read_raw_files,
    validate_pyspark_dataframe,
    write_delta_table,
)
from data_pipeline_prototype.utils.python import (
    resolve_processing_dates,
)
from data_pipeline_prototype.utils.schema.schema_definitions import (
    raw_orders_schema,
)
from data_pipeline_prototype.utils.schema.type_converters import convert_double_to_decimal
from data_pipeline_prototype.utils.spark import spark_session


def to_snake_case(name: str) -> str:
    """Converts a single field name string to snake_case.

    Handles common casing patterns:
        - PascalCase:     OrderId      -> order_id
        - camelCase:      orderId      -> order_id
        - Mixed:          HTTPSRequest -> https_request
        - Spaces/hyphens: Order-Id     -> order_id

    Args:
        name: The field name string to convert.

    Returns:
        The input string converted to snake_case.
    """
    # Normalize separators (spaces, hyphens, dots) to underscores
    name = re.sub(r"[\s\-\.]+", "_", name)
    # Insert underscore between a lowercase/digit and an uppercase letter
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert underscore between consecutive uppercase letters followed by a lowercase
    # e.g. "HTTPSRequest" -> "HTTPS_Request"
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)

    return name.lower()


def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    """Renames all columns in a PySpark DataFrame to snake_case.

    Args:
        df: Input PySpark DataFrame with arbitrarily cased column names.

    Returns:
        A new DataFrame with all column names converted to snake_case.

    Raises:
        ValueError: If renaming produces duplicate column names, which would
            silently corrupt downstream processing.
    """
    new_names = [to_snake_case(col) for col in df.columns]

    if len(new_names) != len(set(new_names)):
        duplicates = [name for name in new_names if new_names.count(name) > 1]
        raise ValueError(
            f"Column renaming produced duplicate names: {set(duplicates)}. "
            "Review source schema for ambiguous field names."
        )

    return df.toDF(*new_names)


def convert_cents_to_euros(df: DataFrame, converted_column: str) -> DataFrame:
    """Converts price columns from cents to Euros, and converts data type to Decimal.

    Args:
        df: Input PySpark DataFrame with a lower case price column.
        converted_column: Name of the column that is being converted from cents to Euros.

    Returns:
        A new PySpark DataFrame with a [converted_column]_eur column, which replaces the original converted column.

    Raises:
        DataQualityError: If the desired price column is not in the PySpark DataFrame.
    """

    result = check_required_columns(df, [converted_column])
    assert_checks_pass([result])

    df = df.withColumn(f"{converted_column}_eur", f.col(converted_column) / c.cents_per_euro).drop(
        converted_column
    )
    df = convert_double_to_decimal(df, f"{converted_column}_eur")

    return df


def include_line_item_dimensions(df: DataFrame) -> DataFrame:
    """Flattens each order's LineItems array into one row per line item.

    inline explodes the array and turns each line item's fields (Sku, ProductName,
    Quantity and UnitPrice) into columns of their own. The order-level columns are
    repeated on each of the order's rows, so the DataFrame's grain changes from one row
    per order to one row per line item, identified by order_id together with Sku.

    UnitPrice is the price of one unit of the SKU in that order, so it can serve as the
    base price per SKU. Order-level amounts such as GrandTotal must not be summed across
    these rows: they repeat once per line item. Orders with an empty LineItems array
    produce no rows.

    Run this before rename_columns_to_snake_case, so the new columns are renamed to
    sku, product_name, quantity and unit_price with the rest.

    Args:
        df: Raw orders with a LineItems array of structs.

    Returns:
        A new DataFrame with one row per line item, the line item fields as columns,
        and without LineItems.
    """

    df = df.select("*", f.inline(c.line_items))

    return df.drop(c.line_items)


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("--ORDERS_INPUT_PATH", type=str)
    parser.add_argument("--ORDERS_PARTITION_KEY", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--START_DATE", type=str)
    parser.add_argument("--END_DATE", type=str)
    parser.add_argument("--LOOKBACK_DAYS", type=int)

    args = parser.parse_known_args(argv)[0]

    return args


def read(args) -> dict:
    """Reads the raw orders for the run's processing dates.

    The processing dates come from PROCESSING_TYPE: the last LOOKBACK_DAYS complete
    days for lookback, or START_DATE to END_DATE for backfill.

    Args:
        args: Parsed job arguments. Uses PROCESSING_TYPE, LOOKBACK_DAYS, START_DATE,
            END_DATE, ORDERS_INPUT_PATH and ORDERS_PARTITION_KEY.

    Returns:
        The raw orders DataFrame, read with raw_orders_schema, keyed by
        orders_dataframe_name.

    Raises:
        ValueError: If the processing arguments are invalid, e.g. an unknown
            PROCESSING_TYPE or a date that is not YYYY-MM-DD.
        FileNotFoundError: If any processing date has no raw data directory. The
            whole run fails before anything is written.
    """
    spark = spark_session()

    processing_dates = resolve_processing_dates(
        processing_type=args.PROCESSING_TYPE,
        lookback_days=args.LOOKBACK_DAYS,
        start_date=args.START_DATE,
        end_date=args.END_DATE,
    )

    orders = read_raw_files(
        spark=spark,
        file_path=args.ORDERS_INPUT_PATH,
        schema=raw_orders_schema,
        date_ranges=processing_dates,
        partition_key=args.ORDERS_PARTITION_KEY,
    )

    return {c.orders_dataframe_name: orders}


def transform(orders: DataFrame) -> DataFrame:
    """Formats raw orders into the preprocessed orders layout, one row per line item.

    Runs these steps in order:
        1. Flattens LineItems into one row per line item, with Sku, ProductName,
           Quantity and UnitPrice as columns (include_line_item_dimensions).
        2. Renames every column to snake_case. This also renames the partition
           column OrderDate to order_date and OrderTimestamp to order_timestamp.
        3. Converts subtotal, discount_total, shipping_total, tax_total and
           grand_total from integer cents to decimal(10,2) <column>_eur columns in
           euros, and unit_price likewise to price_eur.
        4. Adds date_utc, the order's date in UTC, derived from order_timestamp. It
           can differ from order_date: an order placed at 00:30+01:00 on 1 January
           has order_date 1 January but date_utc 31 December.

    Args:
        orders: Raw orders, as returned by read.

    Returns:
        The formatted orders DataFrame, with one row per order line item.

    Raises:
        DataQualityError: If an amount column to convert is missing, or any
            order_id is null.
    """
    orders = include_line_item_dimensions(orders)
    orders = rename_columns_to_snake_case(orders)
    orders = convert_cents_to_euros(orders, c.subtotal)
    orders = convert_cents_to_euros(orders, c.discount_total)
    orders = convert_cents_to_euros(orders, c.shipping_total)
    orders = convert_cents_to_euros(orders, c.tax_total)
    orders = convert_cents_to_euros(orders, c.grand_total)
    orders = convert_cents_to_euros(orders, c.unit_price).withColumnRenamed(
        f"{c.unit_price}_eur", c.price_eur
    )

    result = orders.withColumn(c.date_utc, f.to_date(c.order_timestamp))

    assert_checks_pass(
        [
            check_no_nulls(result, [c.order_id]),
        ]
    )

    return result


def write(
    output_dataframe: DataFrame,
    table_name: str,
    expected_schema: StructType,
) -> None:
    """Validates the formatted orders and writes them to the preprocessed orders table.

    Only the partitions (days) present in `output_dataframe` are replaced; see
    write_delta_table. If validation fails, nothing is written.

    The DataFrame is persisted for the duration of the write because validation and
    the write together run four Spark actions on it: the null count, the row count and
    the distinct-key count in validate_pyspark_dataframe, and the write itself.
    Without persist, each action would recompute the DataFrame from the start, reading
    the raw files from storage again and repeating every transformation. Persisting
    computes it once, and it also guarantees that the data written is exactly the data
    that was validated, even if raw files change in storage during the run.

    Args:
        output_dataframe: The formatted orders, as returned by transform.
        table_name: Table to write to, e.g. preprocessed.orders. It must already exist.
        expected_schema: Schema the DataFrame must match before it is written.

    Raises:
        ValueError: If validation fails: schema mismatch, nulls in non-nullable
            columns, or duplicate order_ids.
        AnalysisException: If the table does not exist or does not match the DataFrame.
    """
    output_dataframe = output_dataframe.persist()

    try:
        validate_pyspark_dataframe(
            output_dataframe,
            expected_schema,
            unique_key=[c.preprocessed_partition_key, c.order_id, c.sku],
        )
        write_delta_table(
            output_dataframe=output_dataframe,
            output_table=table_name,
        )
    finally:
        output_dataframe.unpersist()
