from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType, DoubleType

from data_pipeline_prototype.utils.data_quality import (
    assert_checks_pass,
    check_required_columns,
)


def convert_double_to_decimal(
    df: DataFrame,
    column: str,
    precision: int = 10,
    scale: int = 2,
) -> DataFrame:
    """Converts a DoubleType column in a PySpark DataFrame to DecimalType.

    Args:
        df: The input DataFrame.
        column: Name of the column to convert.
        precision: Total number of digits for the DecimalType.
        scale: Number of digits after the decimal point for the DecimalType.

    Returns:
        DataFrame with the specified column cast to DecimalType(precision, scale).

    Raises:
        DataQualityError: If the column is not present in the DataFrame schema.
        TypeError: If the column exists but is not DoubleType.
    """
    column_check = check_required_columns(df, required=[column])
    assert_checks_pass([column_check])

    if not isinstance(df.schema[column].dataType, DoubleType):
        raise TypeError(
            f"Column '{column}' must be DoubleType for conversion, "
            f"but got {type(df.schema[column].dataType).__name__}."
        )

    return df.withColumn(column, df[column].cast(DecimalType(precision, scale)))
