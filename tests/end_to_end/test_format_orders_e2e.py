"""End-to-end test for format_orders.

Drives the real entrypoint (`run()`) through a hardcoded sys.argv in backfill
mode, then reads the written Delta table back and asserts it matches the expected
output in both schema and content — the composite proof that
parse_arguments -> read -> transform -> write are wired together correctly.
"""

import os

import data_pipeline_prototype.utils.format_orders_constants as c
from data_pipeline_prototype.jobs.format_orders import job
from data_pipeline_prototype.utils.schema.schema_definitions import (
    preprocessed_orders_schema,
)
from tests.helpers import column_types


def test_run_end_to_end(
    spark,
    set_job_arguments,
    create_table,
    integration_fixture_dir,
    processing_date,
    format_orders_expected_output,
):
    raw_dir = os.path.join(integration_fixture_dir, "raw")
    output_table = create_table("preprocessed", "customer_tracking", "orders")

    set_job_arguments(
        "--PROCESSING_TYPE",
        "backfill",
        "--START_DATE",
        processing_date,
        "--END_DATE",
        processing_date,
        "--ORDERS_INPUT_PATH",
        os.path.join(raw_dir, "orders"),
        "--ORDERS_PARTITION_KEY",
        c.raw_partition_key,
        "--OUTPUT_TABLE",
        output_table,
    )

    job.run()

    result = spark.read.table(output_table)

    # (3) the written output conforms to the expected schema
    assert column_types(result.schema) == column_types(preprocessed_orders_schema)

    # (1)+(2) transitively: the end-to-end content equals the known-good result
    actual = sorted(
        result.select(*preprocessed_orders_schema.fieldNames()).collect(),
        key=lambda r: (r[c.order_id], r[c.sku]),
    )
    expected = sorted(
        format_orders_expected_output.collect(), key=lambda r: (r[c.order_id], r[c.sku])
    )

    assert actual == expected
