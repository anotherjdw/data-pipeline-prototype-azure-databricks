import pyspark.sql.types as t

# Raw orders as ingested: one JSON object per order, partitioned by OrderDate directories.
# Field names, nesting and types follow tests/integration/fixtures/raw/orders.
# - Amounts (Subtotal ... GrandTotal, UnitPrice) are integer cents, e.g. 8997 = 89.97 EUR.
# - OrderTimestamp is ISO 8601 with a UTC offset, e.g. 2026-01-01T08:14:52+01:00; reading it as
#   TimestampType applies the offset, so the value is the correct instant in UTC.
# - OrderDate is not in the files: Spark reads it from the OrderDate=<YYYY-MM-DD> directory
#   name (read_raw_files sets basePath). Declaring it here fixes its type instead of inferring it.
# Spark's file readers treat every field as nullable; the nullable flags below document which
# fields are always present in the source and are what validate_pyspark_dataframe checks.
raw_orders_schema = t.StructType(
    [
        t.StructField("OrderId", t.StringType(), nullable=False),
        t.StructField("CustomerId", t.StringType(), nullable=False),
        t.StructField("OrderTimestamp", t.TimestampType(), nullable=False),
        t.StructField("Status", t.StringType(), nullable=False),
        t.StructField("Channel", t.StringType(), nullable=False),
        t.StructField("Currency", t.StringType(), nullable=False),
        t.StructField("Subtotal", t.LongType(), nullable=False),
        t.StructField("DiscountTotal", t.LongType(), nullable=False),
        t.StructField("ShippingTotal", t.LongType(), nullable=False),
        t.StructField("TaxTotal", t.LongType(), nullable=False),
        t.StructField("GrandTotal", t.LongType(), nullable=False),
        # Null while an order is pending payment.
        t.StructField("PaymentMethod", t.StringType(), nullable=True),
        t.StructField("ShippingCountry", t.StringType(), nullable=False),
        t.StructField(
            "LineItems",
            t.ArrayType(
                t.StructType(
                    [
                        t.StructField("Sku", t.StringType(), nullable=False),
                        t.StructField("ProductName", t.StringType(), nullable=False),
                        t.StructField("Quantity", t.LongType(), nullable=False),
                        t.StructField("UnitPrice", t.LongType(), nullable=False),
                    ]
                ),
                containsNull=False,
            ),
            nullable=False,
        ),
        t.StructField("OrderDate", t.DateType(), nullable=False),
    ]
)

# Preprocessed orders: one row per order line item. Mirrors tables/preprocessed/orders.yaml,
# columns first and then the partition key, the same order as the Delta table built from it.
# order_date has no nullable flag in the YAML, so it is nullable, like the table.
preprocessed_orders_schema = t.StructType(
    [
        t.StructField("date_utc", t.DateType(), nullable=False),
        t.StructField("order_id", t.StringType(), nullable=False),
        t.StructField("sku", t.StringType(), nullable=False),
        t.StructField("product_name", t.StringType(), nullable=False),
        t.StructField("quantity", t.LongType(), nullable=False),
        t.StructField("price_eur", t.DecimalType(10, 2), nullable=False),
        t.StructField("customer_id", t.StringType(), nullable=False),
        t.StructField("order_timestamp", t.TimestampType(), nullable=False),
        t.StructField("status", t.StringType(), nullable=False),
        t.StructField("channel", t.StringType(), nullable=False),
        t.StructField("currency", t.StringType(), nullable=False),
        t.StructField("subtotal_eur", t.DecimalType(10, 2), nullable=False),
        t.StructField("discount_total_eur", t.DecimalType(10, 2), nullable=False),
        t.StructField("shipping_total_eur", t.DecimalType(10, 2), nullable=False),
        t.StructField("tax_total_eur", t.DecimalType(10, 2), nullable=False),
        t.StructField("grand_total_eur", t.DecimalType(10, 2), nullable=False),
        t.StructField("payment_method", t.StringType(), nullable=True),
        t.StructField("shipping_country", t.StringType(), nullable=False),
        t.StructField("order_date", t.DateType(), nullable=True),
    ]
)
