"""Glue ETL job: curated (bronze) -> query-ready silver Parquet tables.

The curated layer is written by Firehose as gzip-compressed JSON. This job
reads it back, applies the silver transformation rules, and writes typed,
partitioned Parquet - the layer Athena actually queries.

The stream carries order records wrapped in the ingest envelope
``{"source", "ingested_at", "record"}`` that the Ingestor adds. This job
unwraps the envelope and branches on the source field, writing one table
per source.
"""
from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

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
RAW = f"s3://{BUCKET}/{ARGS['raw-prefix']}"
SILVER = f"s3://{BUCKET}/{ARGS['silver-prefix']}"

# The order record, without the ingest envelope. This is the schema the
# GHN API actually returns: note it has NO lead_time_hours - that value
# does not exist at the source and is computed below.
ORDER_SCHEMA = (
    "order_id string, order_code string, status string, "
    "created_at string, delivered_at string, weight double, "
    "shipping_fee double, cod_amount double, "
    "from_province_id int, from_district_id int, "
    "to_province_id int, to_district_id int"
)


def read_bronze_orders() -> DataFrame:
    """Read the order records, unwrapping the ingest envelope."""
    df = spark.read.json(RAW)
    if "source" in df.columns:
        df = df.filter((F.col("source") == "orders") | F.col("source").isNull())
    unwrapped = df.select(
        F.from_json(F.to_json(F.col("record")), ORDER_SCHEMA).alias("o")
    ).select("o.*")
    return unwrapped.filter(unwrapped["order_code"].isNotNull())


def apply_silver_rules(df: DataFrame) -> DataFrame:
    """Business rules for the silver orders table."""
    out = df.withColumn(
        "is_delivered",
        (F.col("status") == "delivered") & F.col("delivered_at").isNotNull(),
    )
    out = out.withColumn("created_ts", F.to_timestamp(F.col("created_at")))
    out = out.withColumn("delivered_ts", F.to_timestamp(F.col("delivered_at")))
    out = out.withColumn(
        "lead_time_hours",
        F.when(
            F.col("delivered_ts").isNotNull() & F.col("created_ts").isNotNull(),
            (F.unix_timestamp(F.col("delivered_ts")) - F.unix_timestamp(F.col("created_ts")))
            / F.lit(3600.0),
        ).cast("double"),
    )
    out = out.withColumn(
        "lead_time_hours",
        F.greatest(F.col("lead_time_hours"), F.lit(0.0)),
    )
    # Weight is in grams, per GHN's API conventions. 30 000 g (30 kg)
    # is the sensible typo-protection ceiling - anything heavier is a
    # data-entry error, not a parcel.
    out = out.withColumn("weight", F.least(F.col("weight").cast("double"), F.lit(30_000.0)))
    # Deduplicate multiple polled snapshots of the same order_code, keeping the latest state.
    latest_win = Window.partitionBy("order_code").orderBy(
        F.col("is_delivered").desc(),
        F.col("delivered_ts").desc_nulls_last(),
        F.col("created_ts").desc_nulls_last(),
    )
    out = out.withColumn("_rn", F.row_number().over(latest_win)).filter(F.col("_rn") == 1)
    return out.drop("created_ts", "delivered_ts", "_rn")


def main() -> None:
    """Write the silver orders table, partitioned by destination province."""
    orders = read_bronze_orders()
    silver = apply_silver_rules(orders)
    silver.write.mode("overwrite").partitionBy("to_province_id").parquet(
        f"{SILVER}/orders/"
    )
    print(f"silver orders rows={silver.count()}")


if __name__ == "__main__":
    main()
    job.commit()
