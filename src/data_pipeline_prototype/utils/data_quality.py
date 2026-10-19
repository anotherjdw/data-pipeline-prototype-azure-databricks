"""Reusable data quality utility functions for PySpark DataFrames.

Each check returns a CheckResult dataclass rather than raising immediately,
giving the calling ETL job full control over whether to halt, quarantine,
or log violations. Use `assert_checks_pass` to enforce a hard failure at
a defined gate in your job.

Example:
    from utils.data_quality import (
        check_no_nulls,
        check_required_columns,
        check_min_row_count,
        check_primary_key_uniqueness,
        check_no_duplicate_rows,
        check_allowed_values,
        check_cross_column_condition,
        check_value_ranges,
        check_date_column_validity,
        assert_checks_pass,
    )

    results = [
        check_no_nulls(df, columns=["order_id", "customer_id"]),
        check_required_columns(df, required=["order_id", "order_date", "total"]),
        check_min_row_count(df, min_rows=1),
        check_primary_key_uniqueness(df, pk_column="order_id"),
        check_allowed_values(df, column="status", allowed={"pending", "shipped", "closed"}),
        check_cross_column_condition(
            df,
            condition_expr="end_date >= start_date",
            description="end_date must be >= start_date",
        ),
    ]

    assert_checks_pass(results)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result contract
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    """Encapsulates the outcome of a single data quality check.

    Attributes:
        check_name: Human-readable identifier for the check
            (e.g. "no_nulls[order_id]").
        passed: True if the check passed; False if a violation was detected.
        message: A concise description of the outcome, including violation
            counts where relevant.
        details: Optional structured data for downstream logging or alerting
            (e.g. the list of violating values, column name, row counts).
    """

    check_name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.check_name}: {self.message}"


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_no_nulls(
    df: DataFrame,
    columns: list[str],
    null_threshold: float = 0.0,
) -> CheckResult:
    """Asserts that specified columns contain no null values, or that the null
    rate does not exceed a configurable threshold.

    Args:
        df: The DataFrame to validate.
        columns: Column names that must not contain nulls.
        null_threshold: Maximum permitted fraction of null values per column.
            0.0 means zero nulls allowed; 0.05 means up to 5% nulls tolerated.
            Defaults to 0.0.

    Returns:
        A CheckResult that passes only if every specified column is within
        the null threshold.
    """
    check_name = f"no_nulls{columns}"
    total_rows = df.count()

    if total_rows == 0:
        return CheckResult(
            check_name=check_name,
            passed=False,
            message="DataFrame is empty — cannot evaluate null rates.",
            details={"total_rows": 0},
        )

    violations = {}
    for col in columns:
        null_count = df.filter(f.col(col).isNull()).count()
        null_rate = null_count / total_rows
        if null_rate > null_threshold:
            violations[col] = {
                "null_count": null_count,
                "null_rate": round(null_rate, 4),
                "threshold": null_threshold,
            }

    passed = len(violations) == 0
    return CheckResult(
        check_name=check_name,
        passed=passed,
        message=(
            f"All {len(columns)} column(s) are within null threshold."
            if passed
            else f"{len(violations)} column(s) exceed null threshold of "
            f"{null_threshold}: {list(violations.keys())}"
        ),
        details={"violations": violations, "total_rows": total_rows},
    )


def check_required_columns(
    df: DataFrame,
    required: list[str],
) -> CheckResult:
    """Asserts that all required columns are present in the DataFrame schema.

    This is a schema-level check that should run before any transformation
    to catch upstream schema drift early and fail fast.

    Args:
        df: The DataFrame to validate.
        required: Column names that must be present in the schema.

    Returns:
        A CheckResult that passes only if all required columns are present.
    """
    check_name = "required_columns"
    existing = set(df.columns)
    missing = [col for col in required if col not in existing]

    if missing:
        return CheckResult(
            check_name=check_name,
            passed=False,
            message=f"{len(missing)} required column(s) missing from schema: {missing}",
            details={"missing_columns": missing, "existing_columns": list(existing)},
        )

    return CheckResult(
        check_name=check_name,
        passed=True,
        message=f"All {len(required)} required column(s) present in schema.",
    )


def check_min_row_count(
    df: DataFrame,
    min_rows: int = 1,
) -> CheckResult:
    """Asserts that the DataFrame contains at least `min_rows` rows.

    Primarily used to guard against silent empty loads overwriting good data.
    This is a hard floor — relative volume checks (e.g. 'fewer rows than
    yesterday') belong in observability, not here.

    Args:
        df: The DataFrame to validate.
        min_rows: Minimum number of rows required. Defaults to 1 (non-empty).

    Returns:
        A CheckResult that passes only if the row count meets the minimum.
    """
    actual = df.count()
    passed = actual >= min_rows
    return CheckResult(
        check_name=f"min_row_count[>={min_rows}]",
        passed=passed,
        message=(
            f"Row count {actual} meets minimum of {min_rows}."
            if passed
            else f"Row count {actual} is below minimum of {min_rows}."
        ),
        details={"actual_rows": actual, "min_rows": min_rows},
    )


def check_primary_key_uniqueness(
    df: DataFrame,
    pk_column: str,
) -> CheckResult:
    """Asserts that a single column contains no duplicate values.

    Implemented as: total row count == count of distinct values in `pk_column`.

    Args:
        df: The DataFrame to validate.
        pk_column: The column name that must be unique across all rows.

    Returns:
        A CheckResult that passes only if no duplicate values are found
        in the specified column.
    """
    check_name = f"pk_uniqueness[{pk_column}]"
    total_rows = df.count()
    distinct_values = df.select(pk_column).distinct().count()
    duplicate_count = total_rows - distinct_values
    passed = duplicate_count == 0

    return CheckResult(
        check_name=check_name,
        passed=passed,
        message=(
            f"Column '{pk_column}' is unique across all {total_rows} rows."
            if passed
            else f"Column '{pk_column}' contains {duplicate_count} duplicate(s). "
            f"Total rows: {total_rows}, distinct values: {distinct_values}."
        ),
        details={
            "pk_column": pk_column,
            "total_rows": total_rows,
            "distinct_values": distinct_values,
            "duplicate_count": duplicate_count,
        },
    )


def check_no_duplicate_rows(
    df: DataFrame,
    subset: list[str] | None = None,
) -> CheckResult:
    """Asserts that no duplicate rows exist in the DataFrame.

    When `subset` is provided, uniqueness is enforced only across those
    columns — useful for composite business key checks (e.g. one active
    record per customer_id + product_id). When None, all columns are used
    for a full-row deduplication check.

    Args:
        df: The DataFrame to validate.
        subset: Column names to consider for duplicate detection. When None,
            all columns are used. Defaults to None.

    Returns:
        A CheckResult that passes only if no duplicate rows are found
        across the specified columns.
    """
    check_name = f"no_duplicate_rows[{subset or 'all_columns'}]"
    total_rows = df.count()
    distinct_rows = df.dropDuplicates(subset).count() if subset else df.distinct().count()
    duplicate_count = total_rows - distinct_rows
    passed = duplicate_count == 0

    return CheckResult(
        check_name=check_name,
        passed=passed,
        message=(
            f"No duplicate rows found. Total rows: {total_rows}."
            if passed
            else f"{duplicate_count} duplicate row(s) detected across "
            f"{subset or 'all columns'}. Total: {total_rows}, distinct: {distinct_rows}."
        ),
        details={
            "subset": subset or df.columns,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
        },
    )


def check_allowed_values(
    df: DataFrame,
    column: str,
    allowed: set[Any],
    allow_nulls: bool = False,
) -> CheckResult:
    """Asserts that a column contains only values from a defined allowed set.

    Collects the actual distinct values from the column and computes the set
    difference against `allowed`. Values outside the set are reported as
    violations.

    Args:
        df: The DataFrame to validate.
        column: The column name to check.
        allowed: Set of permitted values. Must be non-empty.
        allow_nulls: If False, null values in the column are also treated as
            violations. Defaults to False.

    Returns:
        A CheckResult that passes only if all column values are members of
        the allowed set.
    """
    check_name = f"allowed_values[{column}]"
    actual_values = {row[column] for row in df.select(column).distinct().collect()}
    violations = {v for v in actual_values if v not in allowed and not (allow_nulls and v is None)}
    passed = len(violations) == 0

    return CheckResult(
        check_name=check_name,
        passed=passed,
        message=(
            f"Column '{column}' contains only allowed values."
            if passed
            else f"Column '{column}' contains {len(violations)} disallowed value(s): {violations}."
        ),
        details={
            "column": column,
            "allowed_values": list(allowed),
            "violating_values": list(violations),
        },
    )


def check_value_ranges(
    df: DataFrame,
    column: str,
    min_value: Any | None = None,
    max_value: Any | None = None,
) -> CheckResult:
    """Asserts that all values in a numeric column fall within [min_value, max_value].

    Either bound can be omitted (None) to make the check one-sided.

    Args:
        df: The DataFrame to validate.
        column: The numeric column to check.
        min_value: Lower bound (inclusive). No lower bound check if None.
            Defaults to None.
        max_value: Upper bound (inclusive). No upper bound check if None.
            Defaults to None.

    Returns:
        A CheckResult that passes only if all values fall within the specified
        range. Returns a passing result with a skipped message if neither
        bound is provided.
    """
    check_name = f"value_range[{column}, min={min_value}, max={max_value}]"

    condition = None
    if min_value is not None and max_value is not None:
        condition = ~f.col(column).between(min_value, max_value)
    elif min_value is not None:
        condition = f.col(column) < min_value
    elif max_value is not None:
        condition = f.col(column) > max_value
    else:
        return CheckResult(
            check_name=check_name,
            passed=True,
            message="No range bounds specified — check skipped.",
        )

    violation_count = df.filter(condition | f.col(column).isNull()).count()

    if violation_count > 0:
        return CheckResult(
            check_name=check_name,
            passed=False,
            message=(
                f"Column '{column}' has {violation_count} row(s) outside "
                f"range [{min_value}, {max_value}]."
            ),
            details={
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
                "violation_count": violation_count,
            },
        )

    return CheckResult(
        check_name=check_name,
        passed=True,
        message=f"Column '{column}' values are within range [{min_value}, {max_value}].",
    )


def check_date_column_validity(
    df: DataFrame,
    column: str,
    date_format: str = "yyyy-MM-dd",
    allow_future_dates: bool = True,
) -> CheckResult:
    """Asserts that a string column contains parseable dates in the expected
    format, and optionally that no future dates are present.

    Args:
        df: The DataFrame to validate.
        column: The string column containing date values.
        date_format: Java SimpleDateFormat pattern to validate against.
            Defaults to 'yyyy-MM-dd'.
        allow_future_dates: If False, any date later than today is treated as
            a violation. Defaults to True.

    Returns:
        A CheckResult that passes only if all dates are parseable in the
        specified format and, when allow_future_dates is False, no future
        dates are present.
    """
    check_name = f"date_validity[{column}]"

    parsed_col = "_dq_parsed_date"
    validated = df.withColumn(
        parsed_col,
        f.to_date(f.col(column), date_format),
    )

    unparseable_count = validated.filter(
        f.col(column).isNotNull() & f.col(parsed_col).isNull()
    ).count()

    future_count = 0
    if not allow_future_dates:
        future_count = validated.filter(f.col(parsed_col) > f.current_date()).count()

    total_violations = unparseable_count + future_count

    if total_violations > 0:
        return CheckResult(
            check_name=check_name,
            passed=False,
            message=(
                f"Column '{column}' has {unparseable_count} unparseable date(s) "
                f"and {future_count} future date(s)."
            ),
            details={
                "column": column,
                "date_format": date_format,
                "unparseable_count": unparseable_count,
                "future_date_count": future_count,
            },
        )

    return CheckResult(
        check_name=check_name,
        passed=True,
        message=f"Column '{column}' dates are all valid ({date_format}).",
    )


def check_cross_column_condition(
    df: DataFrame,
    condition_expr: str,
    description: str,
) -> CheckResult:
    """Asserts that a boolean SQL expression holds true for every row.

    The expression is evaluated row-wise using Spark's `expr()`, making it
    suitable for any multi-column business logic rule that cannot be expressed
    as a single-column check.

    Args:
        df: The DataFrame to validate.
        condition_expr: A Spark SQL boolean expression that must be True for
            all rows. Examples:

                - ``"end_date >= start_date"``
                - ``"discount_amount <= total_price"``
                - ``"NOT (status = 'closed' AND closed_date IS NULL)"``
                - ``"quantity * unit_price = line_total"``

        description: A human-readable description of the business rule being
            enforced, used in the result message.

    Returns:
        A CheckResult that passes only if zero rows violate the condition
        expression.
    """
    check_name = f"cross_column[{description}]"
    violation_count = df.filter(~f.expr(condition_expr)).count()
    passed = violation_count == 0

    return CheckResult(
        check_name=check_name,
        passed=passed,
        message=(
            f"All rows satisfy business rule: '{description}'."
            if passed
            else f"Business rule violated: '{description}'. "
            f"{violation_count} row(s) failed condition: `{condition_expr}`."
        ),
        details={
            "condition_expr": condition_expr,
            "description": description,
            "violation_count": violation_count,
        },
    )


# ---------------------------------------------------------------------------
# Gate function
# ---------------------------------------------------------------------------


def assert_checks_pass(
    results: list[CheckResult],
    raise_on_failure: bool = True,
) -> bool:
    """Evaluates a list of CheckResults and optionally raises if any failed.

    All results are logged regardless of outcome. This should be called at a
    defined gate in your ETL job — typically before any write operation.

    Args:
        results: List of CheckResult objects returned by the check functions.
        raise_on_failure: If True, raises a DataQualityError if any check
            failed. Set to False to log-only (useful in non-blocking / warn
            mode). Defaults to True.

    Returns:
        True if all checks passed. False if any checks failed — only
        reachable when raise_on_failure is False.

    Raises:
        DataQualityError: If any check failed and raise_on_failure is True.
    """
    failures = [r for r in results if not r.passed]
    passes = [r for r in results if r.passed]

    for result in passes:
        logger.info(str(result))

    for result in failures:
        logger.error(str(result))

    if failures:
        summary = (
            f"Data quality gate failed: {len(failures)}/{len(results)} check(s) failed. "
            f"Failed checks: {[r.check_name for r in failures]}"
        )
        if raise_on_failure:
            raise DataQualityError(summary, failures=failures)
        else:
            logger.warning(summary)
            return False

    logger.info(f"Data quality gate passed: {len(passes)}/{len(results)} check(s) passed.")
    return True


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class DataQualityError(Exception):
    """Raised when one or more data quality checks fail at a gate.

    Attributes:
        failures: List of CheckResult objects that did not pass.
    """

    def __init__(self, message: str, failures: list[CheckResult]):
        super().__init__(message)
        self.failures = failures
