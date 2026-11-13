from pyspark import pipelines as dp
from pyspark.sql import functions as F

# BRONZE TABLE — holds the raw users data exactly as loaded from S3 (all columns
# are strings). It stays an immutable landing layer, so the original source
# remains available for reprocessing and auditing.
BRONZE_TABLE = "first_project_alex_the_engineer.video1.users_dirty_s3"

# SILVER TABLE — pipeline-owned materialized view holding the cleaned users data:
# real DATE type for signup_date, no duplicates on user_id, no helper columns.
# Downstream analytics and the gold layer can rely on it without re-cleaning.
@dp.materialized_view(name="s3_cleaned_silver")
@dp.expect_or_fail("valid_signup_date", "signup_date IS NOT NULL")
def s3_cleaned_silver():
    bronze = spark.read.table(BRONZE_TABLE)

    # Date fix — step 1: replace dots with slashes to unify the two formats (e.g. "2.29.24" → "2/29/24")
    # Date fix — step 2: convert the string to a real date using the US month/day/2-digit-year format
    # Drop the _rescued_data helper column, auto-created at load time and not part of the business data
    # Remove duplicates: keep only one row per user_id (the only duplicate is USR_1009)
    return (
        bronze
        .withColumn("signup_date", F.to_date(F.regexp_replace("signup_date", "\\.", "/"), "M/d/yy"))
        .drop("_rescued_data")
        .dropDuplicates(["user_id"])
    )
