"""Ingestion stack: EventBridge -> Lambda Ingestor -> S3 raw + Kinesis."""
from __future__ import annotations

import os

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack, StackProps
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam as iam
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sqs as sqs
from constructs import Construct

from cdk_constructs.lambda_function import PipelineLambdaFunction
from stacks.base_stack import BaseStack
from stacks.streaming_stack import StreamingStack


class IngestionStack(Stack):
    """Pulls data from the GHN API and lands it in S3 raw and on the stream."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        streaming: StreamingStack,
        **props: StackProps,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- Secrets: GHN API token lives in Secrets Manager -------------
        # The value is NOT in this repo; it is seeded into Secrets Manager
        # out of band (see docs/runbook.md -> "Seeding secrets").
        self.ghn_secret = secretsmanager.Secret(
            self,
            "GhnApiSecret",
            secret_name=f"{self.stack_name}/ghn-api",
            description="GHN API token. Seeded out of band, never committed.",
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- DLQ: failed API responses land here, never silently dropped --
        self.ingest_dlq = sqs.Queue(
            self,
            "IngestDLQ",
            retention_period=Duration.days(14),
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- Lambda: Ingestor ---------------------------------------------
        self.ingestor = PipelineLambdaFunction(
            self,
            "Ingestor",
            code_path="src/lambda/ingestor",
            timeout=Duration.minutes(2),
            memory_size=512,
            reserved_concurrent_executions=2,
            dead_letter_queue=self.ingest_dlq,
            environment={
                "GHN_SECRET_ARN": self.ghn_secret.secret_arn,
                # The gateway's auth is two headers, Token and ShopId.
                # Overridable per environment.
                "GHN_SHOP_ID": os.environ.get("GHN_SHOP_ID", ""),
                "GHN_BASE_URL": os.environ.get(
                    "GHN_BASE_URL", "https://online-gateway.ghn.vn"
                ),
                "RAW_BUCKET_NAME": base.raw_bucket.bucket_name,
                "KINESIS_STREAM_NAME": streaming.stream.stream_name,
            },
            log_retention=base.log_retention,
        )

        # Read secret, write to raw bucket, publish to the stream
        self.ingestor.add_to_role_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[self.ghn_secret.secret_arn],
            )
        )
        base.raw_bucket.grant_read_write(self.ingestor)
        streaming.stream.grant_write(self.ingestor)

        # --- EventBridge: every 5 minutes ---------------------------------
        self.schedule = events.Rule(
            self,
            "IngestSchedule",
            schedule=events.Schedule.rate(Duration.minutes(5)),
            description="Poll GHN API for orders, shipping status, and master data",
        )
        self.schedule.add_target(targets.LambdaFunction(self.ingestor))

        # --- Master data refresh: daily -----------------------------------
        self.master_data_schedule = events.Rule(
            self,
            "MasterDataSchedule",
            schedule=events.Schedule.cron(hour=2, minute=0),
            description="Daily refresh of provinces, districts, wards",
        )
        self.master_data_schedule.add_target(targets.LambdaFunction(self.ingestor))

        CfnOutput(self, "IngestorFunctionName", value=self.ingestor.function_name)
        CfnOutput(self, "IngestorArn", value=self.ingestor.function_arn)
        CfnOutput(self, "IngestDLQUrl", value=self.ingest_dlq.queue_url)
