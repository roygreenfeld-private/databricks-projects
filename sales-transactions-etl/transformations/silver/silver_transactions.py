from pyspark import pipelines as dp
from pyspark.sql import functions as F

"""Silver layer: cleaned transaction data.

Reads the raw bronze_transactions rows and produces silver_transactions:
- transaction_id / customer_id / product_id: trimmed and uppercased
- product_name / category / store_location / payment_method: trimmed,
  repeated whitespace collapsed to one space, and title-cased
  (fixes values like ' T-SHIRT', 'pHONE cHARGER', 'water  bottle')
- _rescued_data (null in all rows) is dropped
- unit_price and total_amount are rounded to 2 decimals

Data quality: fails the run on missing IDs, drops rows with invalid
quantity or unit_price, and warns if total_amount != quantity x unit_price.
"""


@dp.table(
    name="silver_transactions",
    comment="Cleaned transactions: normalized IDs, text casing and spacing, rounded amounts, rescued data removed",
)
# Fail the update if any transaction_id is null
@dp.expect_or_fail("valid_transaction_id", "transaction_id IS NOT NULL")
# Fail the update if any customer_id is null
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
# Fail the update if any product_id is null
@dp.expect_or_fail("valid_product_id", "product_id IS NOT NULL")
# Drop any row where quantity is less than 1
@dp.expect_or_drop("valid_quantity", "quantity >= 1")
# Drop any row where unit_price is zero or negative
@dp.expect_or_drop("valid_unit_price", "unit_price > 0")
# Warn (but keep the row) when total_amount does not equal quantity x unit_price
@dp.expect(
    "consistent_total_amount",
    "abs(total_amount - (quantity * unit_price)) < 0.01",
)
def silver_transactions():
    # Normalize a text column: trim, collapse repeated whitespace, title-case
    def normalize(col):
        return F.initcap(F.regexp_replace(F.trim(F.col(col)), "\\s+", " "))

    return (
        # Read new rows incrementally from the bronze streaming table
        spark.readStream.table("bronze_transactions")
        # Drop the _rescued_data column (null in all rows, not needed)
        .drop("_rescued_data")
        # Normalize the transaction ID: trim spaces and uppercase
        .withColumn("transaction_id", F.upper(F.trim("transaction_id")))
        # Normalize the customer ID: trim spaces and uppercase
        .withColumn("customer_id", F.upper(F.trim("customer_id")))
        # Normalize the product ID: trim spaces and uppercase
        .withColumn("product_id", F.upper(F.trim("product_id")))
        # Clean the product name: trim, collapse repeated spaces, title-case
        .withColumn("product_name", normalize("product_name"))
        # Clean the category: trim, collapse repeated spaces, title-case
        .withColumn("category", normalize("category"))
        # Clean the store location: trim, collapse repeated spaces, title-case
        .withColumn("store_location", normalize("store_location"))
        # Clean the payment method: trim, collapse repeated spaces, title-case
        .withColumn("payment_method", normalize("payment_method"))
        # Round the unit price to 2 decimal places (cents)
        .withColumn("unit_price", F.round("unit_price", 2))
        # Round the total amount to 2 decimal places (cents)
        .withColumn("total_amount", F.round("total_amount", 2))
    )
