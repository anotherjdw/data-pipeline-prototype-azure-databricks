"""Write-time validation of an output DataFrame against its expected schema.

validate_pyspark_dataframe runs in each job's write(), before the file write. It checks
exact column types (decimal precision/scale included, nullable ignored), then nulls in
non-nullable columns, then uniqueness of the output's key.
"""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_pipeline_prototype.utils.dataframes import validate_pyspark_dataframe

EXPECTED_SCHEMA = StructType(
    [
        StructField("date_utc", DateType(), nullable=False),
        StructField("shop_id", IntegerType(), nullable=False),
        StructField("earnings_eur", DecimalType(10, 2), nullable=True),
    ]
)

# Same names and types, all nullable: lets a test build rows the expected schema forbids.
NULLABLE_SCHEMA = StructType(
    [StructField(field.name, field.dataType, nullable=True) for field in EXPECTED_SCHEMA]
)

UNIQUE_KEY = ["date_utc", "shop_id"]

DAY = date(2026, 1, 1)


def test_valid_dataframe_passes(spark):
    """Correct types, no nulls in non-nullable columns and a unique key raise nothing."""
    df = spark.createDataFrame(
        [
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("10.00")),
            Row(date_utc=DAY, shop_id=2, earnings_eur=None),
        ],
        schema=NULLABLE_SCHEMA,
    )

    validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)


def test_empty_dataframe_passes(spark):
    """An empty DataFrame has no nulls and no duplicates, so it passes."""
    df = spark.createDataFrame([], schema=NULLABLE_SCHEMA)

    validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)


def test_decimal_precision_mismatch_fails(spark):
    """decimal(20,2) is not decimal(10,2): precision and scale are part of the type."""
    schema = StructType(
        [
            StructField("date_utc", DateType()),
            StructField("shop_id", IntegerType()),
            StructField("earnings_eur", DecimalType(20, 2)),
        ]
    )
    df = spark.createDataFrame([Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00"))], schema)

    with pytest.raises(ValueError, match=r"Expected decimal\(10,2\), Received decimal\(20,2\)"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)


def test_missing_column_fails(spark):
    """A column in the expected schema but not in the DataFrame fails schema validation."""
    df = spark.createDataFrame([Row(date_utc=DAY, shop_id=1)], "date_utc date, shop_id int")

    with pytest.raises(
        ValueError, match="(?s)Schema validation failed.*Missing columns.*earnings_eur"
    ):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)


def test_unexpected_column_fails(spark):
    """A column in the DataFrame but not in the expected schema fails schema validation."""
    schema = StructType([*NULLABLE_SCHEMA.fields, StructField("extra", StringType())])
    df = spark.createDataFrame(
        [Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00"), extra="x")], schema
    )

    with pytest.raises(ValueError, match="(?s)Schema validation failed.*Unexpected columns.*extra"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)


def test_null_in_non_nullable_column_fails(spark):
    """A null in a column the expected schema marks non-nullable fails data validation."""
    df = spark.createDataFrame(
        [Row(date_utc=DAY, shop_id=None, earnings_eur=Decimal("1.00"))], schema=NULLABLE_SCHEMA
    )

    with pytest.raises(ValueError, match="Data validation failed: nulls.*shop_id"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)


def test_duplicate_key_fails(spark):
    """Two rows with the same composite key fail data validation."""
    df = spark.createDataFrame(
        [
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00")),
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("2.00")),
        ],
        schema=NULLABLE_SCHEMA,
    )

    with pytest.raises(ValueError, match="Data validation failed: 1 duplicate row"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)


def test_composite_key_is_checked_as_a_whole(spark):
    """Repeated values in one key column are fine while the full key stays unique."""
    df = spark.createDataFrame(
        [
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00")),
            Row(date_utc=DAY, shop_id=2, earnings_eur=Decimal("1.00")),
            Row(date_utc=date(2026, 1, 2), shop_id=1, earnings_eur=Decimal("1.00")),
        ],
        schema=NULLABLE_SCHEMA,
    )

    validate_pyspark_dataframe(df, EXPECTED_SCHEMA, unique_key=UNIQUE_KEY)
