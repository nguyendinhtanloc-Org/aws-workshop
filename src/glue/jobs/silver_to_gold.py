"""Glue ETL job: silver orders -> gold aggregate tables for QuickSight."""
from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

ARGS = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "curated-bucket", "database-name", "silver-prefix", "gold-prefix"],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)

BUCKET = ARGS["curated-bucket"]
SILVER = f"s3://{BUCKET}/{ARGS['silver-prefix']}"
GOLD = f"s3://{BUCKET}/{ARGS['gold-prefix']}"


def build_daily_kpis():
    """Daily delivery KPIs: success rate, lead time, failure rate."""
    df = spark.read.parquet(SILVER)
    return (
        df.groupBy(spark.to_date(spark.col("created_at")).alias("order_date"))
        .agg(
            spark.count("*").alias("total_orders"),
            spark.sum(spark.col("is_delivered").cast("long")).alias("delivered_orders"),
            spark.avg("lead_time_hours").alias("avg_lead_time_hours"),
            spark.sum(
                (spark.col("status") == "failed").cast("long")
            ).alias("failed_orders"),
        )
        .withColumn(
            "success_rate",
            spark.when(spark.col("total_orders") > 0, spark.col("delivered_orders") / spark.col("total_orders")),
        )
        .withColumn(
            "failed_rate",
            spark.when(spark.col("total_orders") > 0, spark.col("failed_orders") / spark.col("total_orders")),
        )
    )


def build_province_kpis():
    """Delivery performance per destination province."""
    df = spark.read.parquet(SILVER)
    return (
        df.groupBy("to_province_id")
        .agg(
            spark.count("*").alias("total_orders"),
            spark.sum(spark.col("is_delivered").cast("long")).alias("delivered_orders"),
            spark.avg("lead_time_hours").alias("avg_lead_time_hours"),
        )
        .withColumn(
            "success_rate",
            spark.when(spark.col("total_orders") > 0, spark.col("delivered_orders") / spark.col("total_orders")),
        )
    )


def main() -> None:
    """Write both gold tables."""
    build_daily_kpis().write.mode("overwrite").parquet(f"{GOLD}/daily_kpis/")
    build_province_kpis().write.mode("overwrite").parquet(f"{GOLD}/province_kpis/")
    print("gold tables written")


if __name__ == "__main__":
    main()
    job.commit()
