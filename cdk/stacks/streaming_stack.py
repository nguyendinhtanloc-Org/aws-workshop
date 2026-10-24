"""Streaming stack: Kinesis Data Streams + Firehose -> S3 curated."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack, StackProps
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk import aws_kinesis as kinesis
from constructs import Construct

from stacks.base_stack import BaseStack


class StreamingStack(Stack):
    """Buffers raw records and writes them to the curated analytics layer."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        **props: StackProps,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- Kinesis Data Streams: decouples ingest rate from delivery rate
        self.stream = kinesis.Stream(
            self,
            "OrdersStream",
            shard_count=2,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.KMS,
            encryption_key=base.curated_data_key,
        )

        # --- Firehose: buffers, compresses, writes Parquet to S3 curated
        self.delivery_stream = firehose.DeliveryStream(
            self,
            "OrdersDeliveryStream",
            source=firehose.KinesisStreamSource(stream=self.stream),
            # JSON, not Parquet. The stream carries three different record
            # shapes (orders, master data, shipping status) and Firehose's
            # Parquet conversion needs one schema for the whole batch -
            # mixed shapes fail conversion and land in the error prefix.
            # Glue reads this JSON layer and writes the typed Parquet
            # silver tables, which is where the schema belongs.
            destination=firehose.S3Bucket(
                base.curated_bucket,
                "curated/",
                encryption_key=base.curated_data_key,
                compression=firehose.Compression.GZIP,
                format=firehose.Format.JSON,
                data_output_prefix="orders/",
                error_output_prefix="errors/",
                file_suffix="",
            ),
            source_buffer_interval=Duration.seconds(60),
            source_buffer_size=firehose.BufferSize.MB_5,
            buffering_size=firehose.BufferSize.MB_64,
            buffering_interval=Duration.minutes(2),
        )

        CfnOutput(self, "DeliveryStreamName", value=self.delivery_stream.delivery_stream_name)
        CfnOutput(self, "KinesisStreamName", value=self.stream.stream_name)
