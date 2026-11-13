from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

# SILVER TABLE — the cleaned users data (real DATE type, one row per user_id)
# published by the bronze-to-silver stage.
SILVER_TABLE = "s3_cleaned_silver"


# BEST DAY OF WEEK TABLE — signup count per weekday, so the business knows which
# days of the week users sign up the most and can plan campaigns and maintenance
# windows around them.
@dp.materialized_view(name="best_day_of_week_gold")
def best_day_of_week_gold():
    silver = spark.read.table(SILVER_TABLE)

    # Count signups per weekday; share of all signups in pct_of_signups, best day first
    counts = (
        silver
        .withColumn("day_of_week", F.date_format("signup_date", "EEEE"))
        .groupBy("day_of_week")
        .agg(F.count("*").alias("total_signups"))
    )
    return (
        counts
        .withColumn(
            "pct_of_signups",
            F.round(F.col("total_signups") * 100.0 / F.sum("total_signups").over(Window.partitionBy()), 1),
        )
        .orderBy(F.desc("total_signups"))
    )


# TOP AD CHANNEL TABLE — signup count per referral/ad channel, so the business
# knows which channels drive the most signups and where to invest marketing budget.
@dp.materialized_view(name="top_ad_channel_gold")
def top_ad_channel_gold():
    silver = spark.read.table(SILVER_TABLE)

    # Count signups per channel; share of all signups in pct_of_signups, top channel first
    counts = (
        silver
        .groupBy("referral_source")
        .agg(F.count("*").alias("total_signups"))
    )
    return (
        counts
        .withColumn(
            "pct_of_signups",
            F.round(F.col("total_signups") * 100.0 / F.sum("total_signups").over(Window.partitionBy()), 1),
        )
        .orderBy(F.desc("total_signups"))
    )


# INSIGHTS GOLD TABLE — combines the weekday and channel views into one table:
# for every weekday × referral-channel combination, how many users signed up and
# from how many distinct countries — showing when, where, and via which channel
# growth happens.
@dp.materialized_view(name="insights_gold")
def insights_gold():
    silver = spark.read.table(SILVER_TABLE)

    # Group by weekday and referral channel: signups and distinct countries per group,
    # biggest groups first
    return (
        silver
        .withColumn("day_name", F.date_format("signup_date", "EEEE"))
        .groupBy("day_name", "referral_source")
        .agg(
            F.count("*").alias("signups"),
            F.countDistinct("country").alias("unique_countries"),
        )
        .orderBy(F.desc("signups"), "day_name", "referral_source")
    )
