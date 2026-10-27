"""Streaming stack: Kinesis Data Streams + Firehose -> S3 curated."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack, StackProps
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_kinesisfirehose as firehose
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
        # One shard: at the Ingestor's volume (scheduled runs writing a few
        # dozen records each) that is ample, and it halves the stream's
        # fixed hourly cost. If throughput ever outgrows it, raise the
        # count and cdk deploy will auto-reshard (a 15-minute freeze).
        self.stream = kinesis.Stream(
            self,
            "OrdersStream",
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.KMS,
            encryption_key=base.curated_data_key,
        )

        # --- Firehose service role: read Kinesis, write S3, use KMS ------
        # The Firehose L1 construct manages the stream itself; the role is
        # what it uses to read the source and write the destination.
        role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            description="Service role for the Firehose delivery stream",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:GetRecords",
                    "kinesis:ListShards",
                    "kinesis:ListShardsForStream",
                    "kinesis:SubscribeToShard",
                    "kinesis:ListStreamConsumers",
                ],
                resources=[self.stream.stream_arn],
            ),
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListBucket"],
                resources=[
                    base.curated_bucket.bucket_arn,
                    base.curated_bucket.bucket_arn + "/*",
                ],
            ),
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "kms:Encrypt",
                    "kms:Decrypt",
                    "kms:GenerateDataKey",
                    "kms:DescribeKey",
                    "kms:CreateGrant",
                ],
                resources=[base.curated_data_key.key_arn],
            ),
        )

        # --- Firehose: buffers, compresses, writes to S3 curated ----------
        # JSON, not Parquet. The stream carries three different record
        # shapes (orders, master data, shipping status) and Firehose's
        # Parquet conversion needs one schema for the whole batch -
        # mixed shapes fail conversion and land in the error prefix.
        # Glue reads this JSON layer and writes the typed Parquet
        # silver tables, which is where the schema belongs.
        # (The kinesisfirehose module ships no L2 construct, so this is
        # an L1 definition with its service role wired in above.)
        self.delivery_stream = firehose.CfnFirehose(
            self,
            "OrdersDeliveryStream",
            delivery_stream_type="KinesisStream",
            kinesis_stream_source_configuration=firehose.CfnFirehose.KinesisStreamSourceConfigurationProperty(
                stream_arn=self.stream.stream_arn,
                role_arn=role.role_arn,
            ),
            # Kinesis-sourced streams only accept the *extended* S3
            # destination (that is where CompressionType / Format /
            # ErrorOutputPrefix / KMSKeyArn live; the plain
            # S3DestinationConfiguration property has none of them).
            extended_s3_destination_configuration=(
                firehose.CfnFirehose.ExtendedS3DestinationConfigurationProperty(
                    bucket_arn=base.curated_bucket.bucket_arn,
                    prefix="curated/orders/",
                    error_output_prefix="curated/errors/",
                    role_arn=role.role_arn,
                    compression_type="GZIP",
                    buffering_size=64,      # MB - flush when the batch hits 64 MB
                    buffering_interval=120,  # or every 2 minutes, whichever first
                    # Records are JSON strings; NONE = raw delivery, GZIP
                    # compresses the batch file. PARQUET/ORC would need a
                    # declared schema - that transform belongs to Glue,
                    # not to Firehose.
                    format="NONE",
                    kms_key_arn=base.curated_data_key.key_arn,  # SSE-KMS
                )
            ),
        )

        # L1 CfnFirehose exposes its physical name via CfnRef (no
        # .delivery_stream_name property), the Kinesis L2 exposes .stream_name.
        CfnOutput(self, "DeliveryStreamName", value=self.delivery_stream.ref)
        CfnOutput(self, "KinesisStreamName", value=self.stream.stream_name)
