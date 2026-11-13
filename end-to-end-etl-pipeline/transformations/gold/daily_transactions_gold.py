from pyspark import pipelines as dp
from pyspark.sql import functions as F

"""Gold layer: daily transaction metrics.

Aggregates the cleaned silver_transactions data into one row per day
(transaction_day, the date part of transaction_date) tracking:
- transaction_count: number of transactions that day
- unique_customers: distinct customer IDs that day
- total_units_sold: sum of quantity
- total_revenue: sum of total_amount
- avg_order_value: average total_amount

This is a materialized view: it refreshes automatically on every
pipeline run, so the daily metrics stay current as new transactions
arrive in silver.
"""


@dp.materialized_view(
    name="daily_transactions_gold",
    comment="Daily transaction metrics: transaction count, revenue, units sold, unique customers and average order value per day",
    cluster_by=["transaction_day"],
)
def daily_transactions_gold():
    return (
        # Read the cleaned silver table as a batch (materialized views recompute from the full table)
        spark.read.table("silver_transactions")
        # Extract the calendar day from each transaction timestamp
        .withColumn("transaction_day", F.to_date("transaction_date"))
        # Group all transactions into one row per day
        .groupBy("transaction_day")
        .agg(
            # Count how many transactions happened that day
            F.count("*").alias("transaction_count"),
            # Count how many distinct customers bought that day
            F.countDistinct("customer_id").alias("unique_customers"),
            # Sum all item quantities sold that day
            F.sum("quantity").alias("total_units_sold"),
            # Sum all transaction amounts for the day's revenue
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            # Average transaction value for that day
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        )
    )
