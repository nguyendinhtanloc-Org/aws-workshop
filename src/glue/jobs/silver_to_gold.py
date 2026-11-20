"""DEPRECATED - do not run. Gold KPIs are now computed by Athena/Trino SQL
(sql/06_build_daily_kpis.sql; SQL is the single source of truth for
ghn_analytics.delivery_kpis). This Glue job wrote a divergent, KPI-mismatched
gold_daily_kpis/gold_province_kpis and was removed from AnalyticsStack.
Kept in-repo for reference only; delete in the next cleanup PR.

Original purpose: Glue ETL job, silver orders -> gold aggregate tables."""
from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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
SILVER = f"s3://{BUCKET}/{ARGS['silver-prefix']}orders/"
GOLD = f"s3://{BUCKET}/{ARGS['gold-prefix']}"


def read_silver_orders() -> DataFrame:
    return spark.read.parquet(SILVER).dropDuplicates(["order_code"])


def build_daily_kpis() -> DataFrame:
    """Daily delivery KPIs: volume, success rate, lead time, failure rate."""
    df = read_silver_orders()
    failed_statuses = ("failed", "delivery_fail", "return_fail", "exception", "lost", "damage", "scrap")
    daily = (
        df.groupBy(F.to_date(F.col("created_at")).alias("order_date"))
        .agg(
            F.count("*").alias("total_orders"),
            F.sum(F.col("is_delivered").cast("long")).alias("delivered_orders"),
            F.avg("lead_time_hours").alias("avg_lead_time_hours"),
            F.sum(F.col("status").isin(*failed_statuses).cast("long")).alias("failed_orders"),
        )
        .withColumn(
            "success_rate",
            F.when(F.col("total_orders") > 0, F.col("delivered_orders") / F.col("total_orders")),
        )
        .withColumn(
            "failed_rate",
            F.when(F.col("total_orders") > 0, F.col("failed_orders") / F.col("total_orders")),
        )
    )
    return daily


def build_province_kpis() -> DataFrame:
    """Delivery performance per destination province."""
    df = read_silver_orders()
    return (
        df.groupBy("to_province_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum(F.col("is_delivered").cast("long")).alias("delivered_orders"),
            F.avg("lead_time_hours").alias("avg_lead_time_hours"),
        )
        .withColumn(
            "success_rate",
            F.when(F.col("total_orders") > 0, F.col("delivered_orders") / F.col("total_orders")),
        )
    )


def main() -> None:
    """Write both gold tables."""
    build_daily_kpis().write.mode("overwrite").parquet(f"{GOLD}daily_kpis/")
    build_province_kpis().write.mode("overwrite").parquet(f"{GOLD}province_kpis/")
    print("gold tables written")


if __name__ == "__main__":
    main()
    job.commit()
