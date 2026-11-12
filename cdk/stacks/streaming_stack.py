"""Streaming stack: Kinesis Data Streams + Firehose -> S3 curated."""
from __future__ import annotations

from typing import Any

from aws_cdk import CfnOutput, Duration, Fn, Stack
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
        **props: Any,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- Kinesis Data Streams: decouples ingest rate from delivery rate
        # One shard: at the Ingestor's volume (scheduled runs writing a few
        # dozen records each) that is ample, and it halves the stream's
        # fixed hourly cost. If throughput ever outgrows it, raise the
        # count and cdk deploy will auto-reshard (a 15-minute freeze).
        # Amazon-managed (not customer) KMS for the stream itself: a custom
        # CMK additionally needs a KMS *key policy* grant for the
        # Kinesis service principal, whose exact form (region-qualified
        # "kinesis:AWS/<stream>") we cannot verify at synth time - miss
        # it and every PutRecord fails with a KMS error even though the
        # stack deploys fine. MANAGED gives SSE for free.
        # (S3 destinations stay customer-KMS: the S3 bucket L2 adds its
        # key-policy grant for the S3 service automatically when it is
        # passed a KMS key.)
        self.stream = kinesis.Stream(
            self,
            "OrdersStream",
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.MANAGED,
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
        self.delivery_stream = firehose.CfnDeliveryStream(
            self,
            "OrdersDeliveryStream",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=self.stream.stream_arn,
                role_arn=role.role_arn,
            ),
            # Kinesis-sourced streams only accept the *extended* S3
            # destination (that is where the encryption configuration
            # and ErrorOutputPrefix live; the plain S3DestinationConfiguration
            # has neither). Property names follow CloudFormation's exact
            # spellings: CompressionFormat - NOT "CompressionType" - and
            # there is no separate "Format" property. Records are already
            # JSON strings, so GZIP only compresses the batch file;
            # Parquet conversion belongs to Glue, not Firehose.
            extended_s3_destination_configuration=(
                firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                    bucket_arn=base.curated_bucket.bucket_arn,
                    prefix="curated/orders/",
                    error_output_prefix="curated/errors/",
                    role_arn=role.role_arn,
                    compression_format="GZIP",
                    # Flush when the batch hits 64 MB or every 2
                    # minutes, whichever comes first.
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=120,
                        size_in_m_bs=64,
                    ),
                    # SSE-KMS is nested under EncryptionConfiguration,
                    # not a top-level KmsKeyArn on the extended S3
                    # destination.
                    encryption_configuration=firehose.CfnDeliveryStream.EncryptionConfigurationProperty(
                        kms_encryption_config=firehose.CfnDeliveryStream.KMSEncryptionConfigProperty(
                            awskms_key_arn=base.curated_data_key.key_arn
                        )
                    ),
                )
            ),
        )

        # L1 CfnDeliveryStream exposes its physical name via CfnRef (no
        # .delivery_stream_name property); Fn.ref takes the logical id,
        # not the construct itself. The Kinesis L2 exposes .stream_name.
        CfnOutput(self, "DeliveryStreamName", value=Fn.ref(self.delivery_stream.logical_id))
        CfnOutput(self, "KinesisStreamName", value=self.stream.stream_name)
