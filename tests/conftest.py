import os
import sys
import time
from collections.abc import Callable, Iterator
from datetime import date, datetime
from decimal import Decimal

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import Row, SparkSession

from data_pipeline_prototype.utils.schema.schema_definitions import preprocessed_orders_schema
from tests.helpers import create_table_statement

os.environ["PYSPARK_PYTHON"] = sys.executable
# collect() turns timestamps into Python datetimes in the process's local time zone, not Spark's
# session time zone. Use UTC so expected values are the same on a laptop and on CI runners.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("data-pipeline-prototype-tests")
        .config("spark.pyspark.python", sys.executable)
        # Delta Lake: Databricks has this built in; locally it comes from the delta-spark package.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # Match Databricks, which renders timestamps in UTC; locally Spark uses the machine's zone.
        .config("spark.sql.session.timeZone", "UTC")
        # Tests create databases and tables; keep Spark's warehouse out of the repo.
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("spark-warehouse")))
    )
    # Adds the Delta JARs matching the installed delta-spark version (downloaded on first run).
    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture
def create_table(spark) -> Iterator[Callable[[str, str, str], str]]:
    """Creates Delta tables from the table YAMLs, and drops their databases after the test.

    In Databricks, Terraform creates every table before a job runs, and write_delta_table
    requires the table to exist. This fixture does the same locally: each call creates
    the <dataset> database and the <dataset>.<table> table defined in
    tables/<layer>/<dataset>/<table>.yaml, and returns the table name, like the job's
    --OUTPUT_TABLE (which in Databricks also starts with the layer's catalog). Tables are
    managed, so their files live in Spark's temporary warehouse and
    DROP DATABASE ... CASCADE removes both catalog entries and files.

    Usage:
        def test_x(spark, create_table):
            orders = create_table("preprocessed", "customer_tracking", "orders")
    """
    databases = set()

    def _create(layer: str, dataset: str, table: str) -> str:
        # No try/except: if the database cannot be created, the test should fail here.
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dataset}")
        databases.add(dataset)
        spark.sql(create_table_statement(layer, dataset, table))
        return f"{dataset}.{table}"

    yield _create

    for database in databases:
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")


@pytest.fixture
def set_job_arguments(monkeypatch) -> Callable[..., None]:
    """Sets sys.argv for a job's run() entrypoint; pytest restores it after the test.

    Usage:
        def test_x(set_job_arguments):
            set_job_arguments("--PROCESSING_TYPE", "backfill", "--START_DATE", "2026-01-01")
    """

    def _set(*arguments: str) -> None:
        # sys.argv[0] is the program name; argparse parses argv[1:] and ignores it.
        monkeypatch.setattr(sys, "argv", ["job.py", *arguments])

    return _set


@pytest.fixture(scope="session")
def processing_date() -> str:
    return "2026-01-01"


@pytest.fixture(scope="session")
def integration_fixture_dir() -> str:
    return os.path.join(os.path.dirname(__file__), "integration", "fixtures")


@pytest.fixture(scope="session")
def format_orders_expected_output(spark):
    """format_orders output for the raw orders fixture: one row per order line item.

    ORD-100240 was placed at 2026-01-01T00:30:00+01:00, so it sits in the OrderDate=2026-01-01
    folder (order_date) but falls on 2025-12-31 in UTC (date_utc).
    """
    return spark.createDataFrame(
        [
            Row(
                date_utc=date(2025, 12, 31),
                order_id="ORD-100240",
                sku="CAP-NVY",
                product_name="Baseball Cap, Navy",
                quantity=2,
                price_eur=Decimal("19.99"),
                customer_id="CUS-90517",
                order_timestamp=datetime(2025, 12, 31, 23, 30, 0),
                status="paid",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("39.98"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("4.95"),
                tax_total_eur=Decimal("6.38"),
                grand_total_eur=Decimal("44.93"),
                payment_method="card",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100241",
                sku="TSH-BLK-M",
                product_name="Basic T-Shirt, Black, M",
                quantity=3,
                price_eur=Decimal("29.99"),
                customer_id="CUS-48213",
                order_timestamp=datetime(2026, 1, 1, 7, 14, 52),
                status="paid",
                channel="web",
                currency="EUR",
                subtotal_eur=Decimal("89.97"),
                discount_total_eur=Decimal("9.00"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("14.36"),
                grand_total_eur=Decimal("80.97"),
                payment_method="card",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100242",
                sku="HOD-GRY-M",
                product_name="Hoodie, Grey, M",
                quantity=1,
                price_eur=Decimal("59.99"),
                customer_id="CUS-51077",
                order_timestamp=datetime(2026, 1, 1, 8, 41, 3),
                status="shipped",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("79.98"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("12.77"),
                grand_total_eur=Decimal("79.98"),
                payment_method="paypal",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100242",
                sku="CAP-NVY",
                product_name="Baseball Cap, Navy",
                quantity=1,
                price_eur=Decimal("19.99"),
                customer_id="CUS-51077",
                order_timestamp=datetime(2026, 1, 1, 8, 41, 3),
                status="shipped",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("79.98"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("12.77"),
                grand_total_eur=Decimal("79.98"),
                payment_method="paypal",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100243",
                sku="SCK-MIX-3",
                product_name="Socks 3-Pack",
                quantity=2,
                price_eur=Decimal("12.99"),
                customer_id="CUS-30982",
                order_timestamp=datetime(2026, 1, 1, 9, 2, 37),
                status="paid",
                channel="web",
                currency="EUR",
                subtotal_eur=Decimal("25.98"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("4.95"),
                tax_total_eur=Decimal("4.15"),
                grand_total_eur=Decimal("30.93"),
                payment_method="klarna",
                shipping_country="AT",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100244",
                sku="JNS-IND-32",
                product_name="Slim Jeans, Indigo, 32",
                quantity=1,
                price_eur=Decimal("79.99"),
                customer_id="CUS-48213",
                order_timestamp=datetime(2026, 1, 1, 10, 26, 15),
                status="cancelled",
                channel="web",
                currency="EUR",
                subtotal_eur=Decimal("79.99"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("12.77"),
                grand_total_eur=Decimal("79.99"),
                payment_method="card",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100245",
                sku="SNK-WHT-42",
                product_name="Canvas Sneakers, White, 42",
                quantity=1,
                price_eur=Decimal("69.99"),
                customer_id="CUS-66410",
                order_timestamp=datetime(2026, 1, 1, 11, 58, 40),
                status="paid",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("82.98"),
                discount_total_eur=Decimal("12.45"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("13.25"),
                grand_total_eur=Decimal("70.53"),
                payment_method="card",
                shipping_country="NL",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100245",
                sku="SCK-MIX-3",
                product_name="Socks 3-Pack",
                quantity=1,
                price_eur=Decimal("12.99"),
                customer_id="CUS-66410",
                order_timestamp=datetime(2026, 1, 1, 11, 58, 40),
                status="paid",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("82.98"),
                discount_total_eur=Decimal("12.45"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("13.25"),
                grand_total_eur=Decimal("70.53"),
                payment_method="card",
                shipping_country="NL",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100246",
                sku="BAG-TOTE",
                product_name="Canvas Tote Bag",
                quantity=1,
                price_eur=Decimal("14.99"),
                customer_id="CUS-12874",
                order_timestamp=datetime(2026, 1, 1, 13, 33, 9),
                status="pending",
                channel="web",
                currency="EUR",
                subtotal_eur=Decimal("14.99"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("4.95"),
                tax_total_eur=Decimal("2.39"),
                grand_total_eur=Decimal("19.94"),
                payment_method=None,
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100247",
                sku="TSH-WHT-L",
                product_name="Basic T-Shirt, White, L",
                quantity=2,
                price_eur=Decimal("29.99"),
                customer_id="CUS-70315",
                order_timestamp=datetime(2026, 1, 1, 15, 7, 21),
                status="shipped",
                channel="web",
                currency="EUR",
                subtotal_eur=Decimal("139.97"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("22.35"),
                grand_total_eur=Decimal("139.97"),
                payment_method="paypal",
                shipping_country="FR",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100247",
                sku="JNS-IND-32",
                product_name="Slim Jeans, Indigo, 32",
                quantity=1,
                price_eur=Decimal("79.99"),
                customer_id="CUS-70315",
                order_timestamp=datetime(2026, 1, 1, 15, 7, 21),
                status="shipped",
                channel="web",
                currency="EUR",
                subtotal_eur=Decimal("139.97"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("22.35"),
                grand_total_eur=Decimal("139.97"),
                payment_method="paypal",
                shipping_country="FR",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100248",
                sku="CAP-NVY",
                product_name="Baseball Cap, Navy",
                quantity=1,
                price_eur=Decimal("19.99"),
                customer_id="CUS-25561",
                order_timestamp=datetime(2026, 1, 1, 17, 45, 58),
                status="refunded",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("19.99"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("4.95"),
                tax_total_eur=Decimal("3.19"),
                grand_total_eur=Decimal("24.94"),
                payment_method="klarna",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100249",
                sku="HOD-GRY-M",
                product_name="Hoodie, Grey, M",
                quantity=2,
                price_eur=Decimal("59.99"),
                customer_id="CUS-39940",
                order_timestamp=datetime(2026, 1, 1, 19, 19, 34),
                status="paid",
                channel="web",
                currency="EUR",
                subtotal_eur=Decimal("119.98"),
                discount_total_eur=Decimal("12.00"),
                shipping_total_eur=Decimal("0.00"),
                tax_total_eur=Decimal("19.16"),
                grand_total_eur=Decimal("107.98"),
                payment_method="card",
                shipping_country="AT",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100250",
                sku="TSH-BLK-M",
                product_name="Basic T-Shirt, Black, M",
                quantity=1,
                price_eur=Decimal("29.99"),
                customer_id="CUS-81236",
                order_timestamp=datetime(2026, 1, 1, 21, 51, 7),
                status="paid",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("57.97"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("4.95"),
                tax_total_eur=Decimal("9.26"),
                grand_total_eur=Decimal("62.92"),
                payment_method="paypal",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100250",
                sku="SCK-MIX-3",
                product_name="Socks 3-Pack",
                quantity=1,
                price_eur=Decimal("12.99"),
                customer_id="CUS-81236",
                order_timestamp=datetime(2026, 1, 1, 21, 51, 7),
                status="paid",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("57.97"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("4.95"),
                tax_total_eur=Decimal("9.26"),
                grand_total_eur=Decimal("62.92"),
                payment_method="paypal",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
            Row(
                date_utc=date(2026, 1, 1),
                order_id="ORD-100250",
                sku="BAG-TOTE",
                product_name="Canvas Tote Bag",
                quantity=1,
                price_eur=Decimal("14.99"),
                customer_id="CUS-81236",
                order_timestamp=datetime(2026, 1, 1, 21, 51, 7),
                status="paid",
                channel="mobile_app",
                currency="EUR",
                subtotal_eur=Decimal("57.97"),
                discount_total_eur=Decimal("0.00"),
                shipping_total_eur=Decimal("4.95"),
                tax_total_eur=Decimal("9.26"),
                grand_total_eur=Decimal("62.92"),
                payment_method="paypal",
                shipping_country="DE",
                order_date=date(2026, 1, 1),
            ),
        ],
        schema=preprocessed_orders_schema,
    )
