from decimal import Decimal

import pytest
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_pipeline_prototype.utils.data_quality import DataQualityError
from data_pipeline_prototype.utils.schema.type_converters import convert_double_to_decimal

INPUT_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("commission_rate", DoubleType(), True),
        StructField("shop_id", IntegerType(), True),
    ]
)

CONVERT_RESULT_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("commission_rate", DecimalType(5, 4), True),
        StructField("shop_id", IntegerType(), True),
    ]
)


@pytest.fixture(scope="module")
def double_df(spark):
    data = [
        ("O1", 0.1234, 1001),
        ("O2", 0.2500, 1002),
        ("O3", 0.0, 1003),
        ("O4", -0.1500, 1004),
        ("O5", None, 1005),
    ]
    return spark.createDataFrame(data, schema=INPUT_SCHEMA)


@pytest.fixture(scope="module")
def convert_result(double_df):
    return convert_double_to_decimal(double_df, "commission_rate", precision=5, scale=4)


# --- convert_double_to_decimal ---


def test_convert_double_to_decimal_happy_path(convert_result):
    """Decimal values are correctly rounded to the specified scale for standard inputs."""
    rows = sorted(convert_result.collect(), key=lambda r: r["order_id"])
    assert rows[0]["commission_rate"] == Decimal("0.1234")
    assert rows[1]["commission_rate"] == Decimal("0.2500")


def test_convert_double_to_decimal_output_schema(convert_result):
    """Output schema matches the expected StructType with DecimalType for the converted column."""
    assert convert_result.schema == CONVERT_RESULT_SCHEMA


def test_convert_double_to_decimal_column_type_is_decimal(convert_result):
    """The converted column has DecimalType in the output schema."""
    assert isinstance(convert_result.schema["commission_rate"].dataType, DecimalType)


def test_convert_double_to_decimal_precision_and_scale(convert_result):
    """The DecimalType has exactly the precision and scale requested."""
    dtype = convert_result.schema["commission_rate"].dataType
    assert dtype.precision == 5
    assert dtype.scale == 4


def test_convert_double_to_decimal_zero_value(convert_result):
    """A DoubleType value of 0.0 is converted to Decimal zero."""
    rows = sorted(convert_result.collect(), key=lambda r: r["order_id"])
    assert rows[2]["commission_rate"] == Decimal("0.0000")


def test_convert_double_to_decimal_negative_value(convert_result):
    """Negative DoubleType values are correctly converted to DecimalType."""
    rows = sorted(convert_result.collect(), key=lambda r: r["order_id"])
    assert rows[3]["commission_rate"] == Decimal("-0.1500")


def test_convert_double_to_decimal_null_preserved(convert_result):
    """Null values in the source column remain null after conversion."""
    rows = sorted(convert_result.collect(), key=lambda r: r["order_id"])
    assert rows[4]["commission_rate"] is None


def test_convert_double_to_decimal_other_columns_unchanged(convert_result):
    """Non-converted columns retain their original types and values."""
    assert convert_result.schema["order_id"].dataType == StringType()
    assert convert_result.schema["shop_id"].dataType == IntegerType()
    rows = sorted(convert_result.collect(), key=lambda r: r["order_id"])
    assert rows[0]["order_id"] == "O1"
    assert rows[0]["shop_id"] == 1001


def test_convert_double_to_decimal_custom_precision_scale(spark):
    """A different precision/scale pair produces the expected DecimalType."""
    data = [("O1", 1234.56789, 1)]
    df = spark.createDataFrame(data, schema=INPUT_SCHEMA)
    result = convert_double_to_decimal(df, "commission_rate", precision=10, scale=2)
    dtype = result.schema["commission_rate"].dataType
    assert dtype.precision == 10
    assert dtype.scale == 2
    assert result.collect()[0]["commission_rate"] == Decimal("1234.57")


def test_convert_double_to_decimal_raises_on_missing_column(double_df):
    """DataQualityError is raised when the specified column is not in the DataFrame."""
    with pytest.raises(DataQualityError):
        convert_double_to_decimal(double_df, "nonexistent_column", precision=5, scale=4)


def test_convert_double_to_decimal_raises_on_non_double_column(double_df):
    """TypeError is raised when the target column exists but is not DoubleType."""
    with pytest.raises(TypeError, match="must be DoubleType"):
        convert_double_to_decimal(double_df, "order_id", precision=5, scale=4)


def test_convert_double_to_decimal_raises_type_error_message_includes_actual_type(double_df):
    """TypeError message names the actual type of the offending column."""
    with pytest.raises(TypeError, match="StringType"):
        convert_double_to_decimal(double_df, "order_id", precision=5, scale=4)


def test_convert_double_to_decimal_large_value(spark):
    """Large DoubleType values are correctly cast to DecimalType without truncation of integer digits."""
    schema = StructType([StructField("amount", DoubleType(), True)])
    df = spark.createDataFrame([(999999.99,)], schema=schema)
    result = convert_double_to_decimal(df, "amount", precision=10, scale=2)
    assert result.collect()[0]["amount"] == Decimal("999999.99")
