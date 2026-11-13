"""Bronze layer: raw ingestion of transaction data.

Reads rows as-is from the source streaming table
first_project_alex_the_engineer.endtoend.transactions into the
bronze_transactions streaming table. No cleaning happens in this
layer -- bronze keeps the raw data untouched so any silver/gold issue
can always be traced back to what the source actually delivered.
"""
from pyspark import pipelines as dp


@dp.table(
    name="bronze_transactions",
    comment="Raw transaction data ingested as-is from first_project_alex_the_engineer.endtoend.transactions",
)
def bronze_transactions():
    # Stream new rows incrementally from the source table, exactly as-is (no cleaning in bronze)
    return spark.readStream.table("first_project_alex_the_engineer.endtoend.transactions")
