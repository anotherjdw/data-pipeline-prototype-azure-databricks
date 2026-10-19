"""read_raw_files, read_delta_table and write_delta_table against local files and Delta tables.

Raw files are written to tmp_path per test. Delta tables come from the create_table
fixture, built from the same table YAML Terraform uses, and dropped after each test.
Each test compares the actual DataFrame with an expected DataFrame: schema, then rows.
"""

import json
from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, StringType, StructField, StructType

from data_pipeline_prototype.utils.dataframes import (
    read_delta_table,
    read_raw_files,
    write_delta_table,
)
from tests.helpers import column_types

RAW_ORDERS_SCHEMA = StructType(
    [
        StructField("OrderId", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("OrderDate", DateType(), True),
    ]
)

RAW_ORDER_IDS_SCHEMA = StructType(
    [
        StructField("OrderId", StringType(), True),
        StructField("OrderDate", DateType(), True),
    ]
)

# --- helpers ---


def write_raw_day(base, partition_key: str, day: str, order_ids: list[str]) -> None:
    """Writes one day of raw orders as a multi-line JSON file, like the ingested data."""
    directory = base / f"{partition_key}={day}"
    directory.mkdir(parents=True)
    records = [{"OrderId": order_id, "Status": "paid"} for order_id in order_ids]
    (directory / "orders_data.json").write_text(json.dumps(records))


def orders_dataframe(spark, table: str, rows: list[tuple[str, str]]) -> DataFrame:
    """Builds rows in the table's own schema from (order_id, order_date) pairs.

    Every other column gets a fixed valid value, so tests only spell out what they check.
    """
    schema = spark.read.table(table).schema

    def full_row(order_id: str, day: str) -> tuple:
        values = {
            "order_id": order_id,
            "sku": "SKU-1",
            "product_name": "Product 1",
            "quantity": 1,
            "price_eur": Decimal("10.00"),
            "customer_id": "CUS-1",
            "order_timestamp": datetime.fromisoformat(f"{day}T12:00:00"),
            "status": "paid",
            "channel": "web",
            "currency": "EUR",
            "subtotal_eur": Decimal("10.00"),
            "discount_total_eur": Decimal("0.00"),
            "shipping_total_eur": Decimal("0.00"),
            "tax_total_eur": Decimal("1.60"),
            "grand_total_eur": Decimal("10.00"),
            "payment_method": "card",
            "shipping_country": "DE",
            "date_utc": date.fromisoformat(day),
            "order_date": date.fromisoformat(day),
        }
        return tuple(values[field.name] for field in schema.fields)

    return spark.createDataFrame([full_row(*row) for row in rows], schema)


@pytest.fixture
def raw_orders(tmp_path):
    """Raw orders for three days, 2026-01-01 to 2026-01-03, partitioned by OrderDate."""
    base = tmp_path / "orders"
    write_raw_day(base, "OrderDate", "2026-01-01", ["a1", "a2"])
    write_raw_day(base, "OrderDate", "2026-01-02", ["b1"])
    write_raw_day(base, "OrderDate", "2026-01-03", ["c1"])
    return str(base)


@pytest.fixture
def orders_table(spark, create_table) -> str:
    """customer_tracking.orders with one order on each day from 2026-01-01 to 2026-01-04."""
    table = create_table("preprocessed", "customer_tracking", "orders")
    rows = [("a", "2026-01-01"), ("b", "2026-01-02"), ("c", "2026-01-03"), ("d", "2026-01-04")]
    orders_dataframe(spark, table, rows).writeTo(table).append()
    return table


# --- read_raw_files ---


def test_read_raw_files_reads_only_requested_days(spark, raw_orders):
    """Only the requested days are read, and OrderDate is kept as a date column."""
    result = read_raw_files(
        spark, raw_orders, date_ranges=["2026-01-01", "2026-01-02"], partition_key="OrderDate"
    ).select("OrderId", "Status", "OrderDate")

    expected = spark.createDataFrame(
        [
            ("a1", "paid", date(2026, 1, 1)),
            ("a2", "paid", date(2026, 1, 1)),
            ("b1", "paid", date(2026, 1, 2)),
        ],
        schema=RAW_ORDERS_SCHEMA,
    )

    assert column_types(result.schema) == column_types(expected.schema)

    actual = sorted(result.collect(), key=lambda r: r["OrderId"])
    expected = sorted(expected.collect(), key=lambda r: r["OrderId"])

    assert actual == expected


def test_read_raw_files_applies_schema(spark, raw_orders):
    """Status is in the files but not the schema, so it is dropped; OrderDate is appended."""
    schema = StructType([StructField("OrderId", StringType(), True)])

    result = read_raw_files(
        spark, raw_orders, schema=schema, date_ranges=["2026-01-01"], partition_key="OrderDate"
    )

    expected = spark.createDataFrame(
        [("a1", date(2026, 1, 1)), ("a2", date(2026, 1, 1))], schema=RAW_ORDER_IDS_SCHEMA
    )

    assert result.columns == expected.columns
    assert column_types(result.schema) == column_types(expected.schema)

    actual = sorted(result.collect(), key=lambda r: r["OrderId"])
    expected = sorted(expected.collect(), key=lambda r: r["OrderId"])

    assert actual == expected


def test_read_raw_files_without_partition_key_reads_everything(spark, raw_orders):
    """Without partition_key and date_ranges, every day under file_path is read."""
    result = read_raw_files(spark, raw_orders).select("OrderId", "Status", "OrderDate")

    expected = spark.createDataFrame(
        [
            ("a1", "paid", date(2026, 1, 1)),
            ("a2", "paid", date(2026, 1, 1)),
            ("b1", "paid", date(2026, 1, 2)),
            ("c1", "paid", date(2026, 1, 3)),
        ],
        schema=RAW_ORDERS_SCHEMA,
    )

    actual = sorted(result.collect(), key=lambda r: r["OrderId"])
    expected = sorted(expected.collect(), key=lambda r: r["OrderId"])

    assert actual == expected


def test_read_raw_files_missing_day_fails_whole_read(spark, raw_orders):
    with pytest.raises(FileNotFoundError, match="OrderDate=2026-01-04"):
        read_raw_files(
            spark, raw_orders, date_ranges=["2026-01-03", "2026-01-04"], partition_key="OrderDate"
        )


def test_read_raw_files_rejects_invalid_date(spark, raw_orders):
    with pytest.raises(ValueError, match="Invalid dates"):
        read_raw_files(spark, raw_orders, date_ranges=["01/01/2026"], partition_key="OrderDate")


# --- read_delta_table ---


def test_read_delta_table_reads_every_day_in_range(spark, orders_table):
    """Three days, so the day between start_date and end_date is checked too."""
    result = read_delta_table(spark, orders_table, "2026-01-01", "2026-01-03", "order_date")

    expected = orders_dataframe(
        spark, orders_table, [("a", "2026-01-01"), ("b", "2026-01-02"), ("c", "2026-01-03")]
    )

    assert column_types(result.schema) == column_types(expected.schema)

    actual = sorted(result.collect(), key=lambda r: r["order_id"])
    expected = sorted(expected.collect(), key=lambda r: r["order_id"])

    assert actual == expected


def test_read_delta_table_single_day(spark, orders_table):
    result = read_delta_table(spark, orders_table, "2026-01-02", "2026-01-02", "order_date")

    expected = orders_dataframe(spark, orders_table, [("b", "2026-01-02")])

    assert result.collect() == expected.collect()


def test_read_delta_table_without_partition_key_reads_whole_table(spark, orders_table):
    result = read_delta_table(spark, orders_table)

    expected = orders_dataframe(
        spark,
        orders_table,
        [("a", "2026-01-01"), ("b", "2026-01-02"), ("c", "2026-01-03"), ("d", "2026-01-04")],
    )

    actual = sorted(result.collect(), key=lambda r: r["order_id"])
    expected = sorted(expected.collect(), key=lambda r: r["order_id"])

    assert actual == expected


def test_read_delta_table_days_without_data_return_no_rows(spark, orders_table):
    result = read_delta_table(spark, orders_table, "2026-01-05", "2026-01-06", "order_date")

    expected = orders_dataframe(spark, orders_table, [])

    assert column_types(result.schema) == column_types(expected.schema)
    assert result.collect() == expected.collect()


@pytest.mark.parametrize(
    ("start_date", "end_date", "partition_key", "message"),
    [
        ("2026-01-01", None, "order_date", "must be provided"),
        (None, "2026-01-01", "order_date", "must be provided"),
        ("2026-01-03", "2026-01-01", "order_date", "is after end_date"),
        ("01/01/2026", "2026-01-02", "order_date", "Invalid dates"),
        ("2026-01-01", "2026-01-02", None, "partition_key is required"),
    ],
    ids=["start-only", "end-only", "start-after-end", "invalid-format", "dates-without-key"],
)
def test_read_delta_table_rejects_invalid_arguments(
    spark, orders_table, start_date, end_date, partition_key, message
):
    with pytest.raises(ValueError, match=message):
        read_delta_table(spark, orders_table, start_date, end_date, partition_key)


# --- write_delta_table ---


def test_write_delta_table_replaces_only_written_partitions(spark, orders_table):
    write_delta_table(orders_dataframe(spark, orders_table, [("a2", "2026-01-01")]), orders_table)

    expected = orders_dataframe(
        spark,
        orders_table,
        [("a2", "2026-01-01"), ("b", "2026-01-02"), ("c", "2026-01-03"), ("d", "2026-01-04")],
    )

    actual = sorted(spark.read.table(orders_table).collect(), key=lambda r: r["order_id"])
    expected = sorted(expected.collect(), key=lambda r: r["order_id"])

    assert actual == expected


def test_write_delta_table_rerun_gives_same_result(spark, orders_table):
    rerun = orders_dataframe(spark, orders_table, [("a2", "2026-01-01")])

    write_delta_table(rerun, orders_table)
    write_delta_table(rerun, orders_table)

    expected = orders_dataframe(
        spark,
        orders_table,
        [("a2", "2026-01-01"), ("b", "2026-01-02"), ("c", "2026-01-03"), ("d", "2026-01-04")],
    )

    actual = sorted(spark.read.table(orders_table).collect(), key=lambda r: r["order_id"])
    expected = sorted(expected.collect(), key=lambda r: r["order_id"])

    assert actual == expected


def test_write_delta_table_empty_dataframe_changes_nothing(spark, orders_table):
    before = sorted(spark.read.table(orders_table).collect(), key=lambda r: r["order_id"])

    write_delta_table(orders_dataframe(spark, orders_table, []), orders_table)

    actual = sorted(spark.read.table(orders_table).collect(), key=lambda r: r["order_id"])

    assert actual == before


def test_write_delta_table_missing_table_fails(spark, orders_table):
    data = orders_dataframe(spark, orders_table, [("a2", "2026-01-01")])

    with pytest.raises(AnalysisException, match="TABLE_OR_VIEW_NOT_FOUND"):
        write_delta_table(data, "customer_tracking.does_not_exist")


def test_write_delta_table_rejects_unexpected_column(spark, orders_table):
    before = sorted(spark.read.table(orders_table).collect(), key=lambda r: r["order_id"])
    data = orders_dataframe(spark, orders_table, [("a2", "2026-01-01")]).withColumnRenamed(
        "status", "order_status"
    )

    with pytest.raises(AnalysisException):
        write_delta_table(data, orders_table)

    actual = sorted(spark.read.table(orders_table).collect(), key=lambda r: r["order_id"])

    assert actual == before
