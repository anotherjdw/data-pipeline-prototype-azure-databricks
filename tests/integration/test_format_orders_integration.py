import os

import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import data_pipeline_prototype.utils.format_orders_constants as c
from data_pipeline_prototype.transformations.format_orders_transformations import (
    parse_arguments,
    read,
    transform,
    write,
)
from data_pipeline_prototype.utils.schema.schema_definitions import (
    preprocessed_orders_schema,
    raw_orders_schema,
)
from tests.helpers import column_types


def _sort_key(row):
    return (row[c.order_id], row[c.sku])


@pytest.fixture(scope="session")
def test_args(integration_fixture_dir, processing_date):

    raw_dir = os.path.join(integration_fixture_dir, "raw")

    return parse_arguments(
        [
            "--ORDERS_INPUT_PATH",
            os.path.join(raw_dir, "orders"),
            "--PROCESSING_TYPE",
            "backfill",
            "--START_DATE",
            processing_date,
            "--END_DATE",
            processing_date,
            "--ORDERS_PARTITION_KEY",
            c.raw_partition_key,
            "--OUTPUT_TABLE",
            "customer_tracking.orders",
        ]
    )


@pytest.fixture(scope="session")
def read_output(test_args):

    return read(test_args)


def test_read(spark, test_args, read_output, integration_fixture_dir):

    raw_dir = os.path.join(integration_fixture_dir, "raw")

    args_incorrect_date = parse_arguments(
        [
            "--ORDERS_INPUT_PATH",
            os.path.join(raw_dir, "orders"),
            "--PROCESSING_TYPE",
            "backfill",
            "--START_DATE",
            "2026-01-02",
            "--END_DATE",
            "2026-01-02",
            "--ORDERS_PARTITION_KEY",
            c.raw_partition_key,
            "--OUTPUT_TABLE",
            "customer_tracking.orders",
        ]
    )

    orders = read_output[c.orders_dataframe_name]

    assert set(read_output.keys()) == {c.orders_dataframe_name}

    assert orders.count() > 0

    # Spark reads every column from files as nullable, nested LineItems fields included, so
    # compare names and types only: simpleString leaves out nullability at every level.
    assert orders.schema.simpleString() == raw_orders_schema.simpleString()

    # A day without raw data fails the whole read rather than returning no rows.
    with pytest.raises(FileNotFoundError, match="OrderDate=2026-01-02"):
        read(args_incorrect_date)


def test_transform(read_output, format_orders_expected_output):

    orders = read_output[c.orders_dataframe_name]

    actual = transform(orders).select(*preprocessed_orders_schema.fieldNames())
    actual = sorted(actual.collect(), key=_sort_key)

    expected = sorted(format_orders_expected_output.collect(), key=_sort_key)

    assert actual == expected


def test_write(spark, create_table, format_orders_expected_output, processing_date):

    table = create_table("preprocessed", "customer_tracking", "orders")

    write(
        output_dataframe=format_orders_expected_output,
        table_name=table,
        expected_schema=preprocessed_orders_schema,
    )

    result = spark.read.table(table)

    assert column_types(result.schema) == column_types(preprocessed_orders_schema)

    assert result.count() == format_orders_expected_output.count()
    assert result.filter(f.col(c.order_id).isNull()).count() == 0
    assert result.filter(f.col(c.sku).isNull()).count() == 0
    assert result.filter(f.col(c.price_eur).isNull()).count() == 0
    assert result.filter(f.col(c.date_utc).isNull()).count() == 0
    assert result.filter(f.col(c.preprocessed_partition_key).isNull()).count() == 0

    partitions = [
        row[c.preprocessed_partition_key].isoformat()
        for row in result.select(c.preprocessed_partition_key).distinct().collect()
    ]
    assert partitions == [processing_date]


def test_write_keeps_other_partitions(spark, create_table, format_orders_expected_output):

    table = create_table("preprocessed", "customer_tracking", "orders")

    # The previous day's run already wrote order_date 2025-12-31. ORD-100240 also has
    # date_utc 2025-12-31, but it belongs to order_date 2026-01-01, so writing it must not
    # replace the 2025-12-31 partition.
    previous_day = format_orders_expected_output.withColumn(
        c.preprocessed_partition_key, f.date_sub(f.col(c.preprocessed_partition_key), 1)
    )

    write(
        output_dataframe=previous_day,
        table_name=table,
        expected_schema=preprocessed_orders_schema,
    )

    # Run the 2026-01-01 write twice: a rerun must replace its partition, not append to it.
    for _ in range(2):
        write(
            output_dataframe=format_orders_expected_output,
            table_name=table,
            expected_schema=preprocessed_orders_schema,
        )

    result = spark.read.table(table)

    assert result.count() == 2 * format_orders_expected_output.count()
    assert sorted(
        row[c.preprocessed_partition_key].isoformat()
        for row in result.select(c.preprocessed_partition_key).distinct().collect()
    ) == [
        "2025-12-31",
        "2026-01-01",
    ]


def test_write_incorrect_schema(format_orders_expected_output):
    incorrect_schema = StructType(
        [
            StructField("date_utc", DateType(), nullable=True),
            StructField("order_id", StringType(), nullable=True),
            StructField("sku", StringType(), nullable=True),
            StructField("product_name", StringType(), nullable=True),
            StructField("quantity", LongType(), nullable=True),
            StructField("price_eur", DecimalType(10, 2), nullable=True),
            StructField("customer_id", StringType(), nullable=True),
            StructField("order_timestamp", TimestampType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("channel", StringType(), nullable=True),
            StructField("currency", StringType(), nullable=True),
            StructField("subtotal_eur", DecimalType(10, 2), nullable=True),
            StructField("discount_total_eur", DecimalType(10, 2), nullable=True),
            StructField("shipping_total_eur", DecimalType(10, 2), nullable=True),
            StructField("tax_total_eur", DecimalType(10, 2), nullable=True),
            StructField("grand_total_eur", DecimalType(10, 2), nullable=True),
            StructField("payment_method", StringType(), nullable=True),
            StructField("shipping_country", StringType(), nullable=True),
            StructField("order_date", StringType(), nullable=True),
        ]
    )

    with pytest.raises(ValueError, match="Schema validation failed"):
        write(
            output_dataframe=format_orders_expected_output,
            table_name="customer_tracking.orders",
            expected_schema=incorrect_schema,
        )
