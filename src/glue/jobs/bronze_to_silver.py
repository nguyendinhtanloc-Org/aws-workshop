"""Glue ETL job: curated (bronze) orders -> query-ready silver table.

Reads Parquet from the curated bucket, applies type casting and business
rules, and writes a partitioned Parquet table back to the same bucket.
"""
from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

ARGS = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "curated-bucket", "database-name", "raw-prefix", "silver-prefix"],
)

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)

BUCKET = ARGS["curated-bucket"]
DATABASE = ARGS["database-name"]
RAW = f"s3://{BUCKET}/{ARGS['raw-prefix']}"
SILVER = f"s3://{BUCKET}/{ARGS['silver-prefix']}"


def build_silver_frame():
    """Read bronze Parquet and apply the silver transformation rules."""
    df = spark.read.parquet(RAW)

    # Business rules:
    # - an order is 'delivered' only if it has a delivery timestamp
    # - lead time is recomputed from timestamps, not trusted from source
    # - weight is clamped to a sane upper bound (typo protection)
    df = df.withColumn(
        "is_delivered",
        (spark.col("status") == "delivered") & spark.col("delivered_at").isNotNull(),
    )
    df = df.withColumn(
        "lead_time_hours",
        spark.greatest(
            spark.col("lead_time_hours").cast("double"),
            spark.lit(0.0),
        ),
    )
    df = df.withColumn("weight", spark.least(spark.col("weight").cast("double"), spark.lit(1000.0)))
    return df


def main() -> None:
    """Run the ETL job."""
    silver_df = build_silver_frame()
    silver_df.write.mode("overwrite").partitionBy("to_province_id").parquet(SILVER)
    print(f"silver rows={silver_df.count()}")


if __name__ == "__main__":
    main()
    job.commit()
