"""GHN webhook, daily reconciliation and master data refresh."""

from __future__ import annotations

import os
from typing import Any

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_secretsmanager as secretsmanager
from aws_cdk import aws_sqs as sqs
from cdk_constructs.lambda_function import PipelineLambdaFunction
from constructs import Construct

from stacks.base_stack import BaseStack


class IngestionStack(Stack):
    """Pulls data from the GHN API and lands it in S3 raw; webhook and reconciliation deliver to SQS."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        **props: Any,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        self.ghn_secret = secretsmanager.Secret(
            self,
            "GhnApiSecret",
            secret_name=f"{self.stack_name}-ghn-api",
            description="GHN API token. Seeded out of band, never committed.",
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.ingest_dlq = sqs.Queue(
            self,
            "IngestDLQ",
            retention_period=Duration.days(14),
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.master_data = PipelineLambdaFunction(
            self,
            "Ingestor",
            code_path="build/lambda-ingestor",
            timeout=Duration.minutes(2),
            memory_size=512,
            reserved_concurrent_executions=2,
            dead_letter_queue=self.ingest_dlq,
            environment={
                "GHN_SECRET_ARN": self.ghn_secret.secret_arn,
                "GHN_SHOP_ID": os.environ.get("GHN_SHOP_ID", ""),
                "GHN_BASE_URL": os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn"),
                "GHN_WARD_DISTRICTS": os.environ.get("GHN_WARD_DISTRICTS", ""),
                "RAW_BUCKET_NAME": base.raw_bucket.bucket_name,
            },
            log_retention=base.log_retention,
        )

        self.ghn_secret.grant_read(self.master_data)
        base.raw_bucket.grant_put(self.master_data, "raw/master_data/*")

        self.master_data_schedule = events.Rule(
            self,
            "MasterDataSchedule",
            schedule=events.Schedule.cron(hour="2", minute="0"),
            description="Daily refresh of provinces, districts, wards",
        )
        self.master_data_schedule.add_target(
            targets.LambdaFunction(
                self.master_data,
                event=events.RuleTargetInput.from_object({"sources": ["master_data"]}),
            )
        )

        self.webhook_dlq = sqs.Queue(
            self,
            "WebhookDLQ",
            fifo=True,
            retention_period=Duration.days(14),
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.webhook_queue = sqs.Queue(
            self,
            "WebhookQueue",
            fifo=True,
            visibility_timeout=Duration.minutes(12),
            retention_period=Duration.days(14),
            enforce_ssl=True,
            dead_letter_queue=sqs.DeadLetterQueue(queue=self.webhook_dlq, max_receive_count=5),
        )
        self.recon_state = dynamodb.Table(
            self,
            "ReconciliationState",
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.webhook_secret = secretsmanager.Secret(self, "WebhookSecret")
        shared = {
            "RAW_BUCKET_NAME": base.raw_bucket.bucket_name,
            "WEBHOOK_SQS_URL": self.webhook_queue.queue_url,
            "ORDER_WATCHLIST_TABLE": base.watchlist_table.table_name,
        }
        self.webhook = PipelineLambdaFunction(
            self,
            "WebhookReceiver",
            code_path="build/lambda-ingestor",
            handler="webhook_handler.lambda_handler",
            log_retention=base.log_retention,
            environment={
                **shared,
                "GHN_WEBHOOK_SECRET_ARN": self.webhook_secret.secret_arn,
            },
        )
        self.webhook_secret.grant_read(self.webhook)
        self.api = apigateway.LambdaRestApi(self, "WebhookApi", handler=self.webhook, proxy=False)
        self.api.root.add_resource("webhook").add_method("POST")
        self.reconciler = PipelineLambdaFunction(
            self,
            "Reconciler",
            code_path="build/lambda-ingestor",
            handler="reconciler.lambda_handler",
            timeout=Duration.minutes(15),
            reserved_concurrent_executions=2,
            dead_letter_queue=self.ingest_dlq,
            log_retention=base.log_retention,
            environment={
                **shared,
                "RECON_STATE_TABLE": self.recon_state.table_name,
                "ORDERS_TABLE_NAME": base.orders_table.table_name,
                "GHN_SECRET_ARN": self.ghn_secret.secret_arn,
                "GHN_SHOP_ID": os.environ.get("GHN_SHOP_ID", ""),
                "GHN_BASE_URL": os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn"),
            },
        )
        self.ghn_secret.grant_read(self.reconciler)
        self.recon_state.grant_read_write_data(self.reconciler)
        base.orders_table.grant_read_data(self.reconciler)
        for function in (self.webhook, self.reconciler):
            base.raw_bucket.grant_put(function)
            base.watchlist_table.grant_read_write_data(function)
            self.webhook_queue.grant_send_messages(function)
        self.recon_schedule = events.Rule(
            self,
            "ReconciliationSchedule",
            schedule=events.Schedule.cron(hour="20", minute="0"),
            description="Daily reconciliation at 03:00 Asia/Ho_Chi_Minh",
        )
        self.recon_schedule.add_target(targets.LambdaFunction(self.reconciler))
        CfnOutput(self, "WebhookUrl", value=self.api.url + "webhook")
        CfnOutput(self, "WebhookQueueUrl", value=self.webhook_queue.queue_url)
        CfnOutput(self, "WebhookQueueArn", value=self.webhook_queue.queue_arn)
        CfnOutput(self, "ReconcilerName", value=self.reconciler.function_name)

        CfnOutput(self, "MasterDataFunctionName", value=self.master_data.function_name)
        CfnOutput(self, "IngestDLQUrl", value=self.ingest_dlq.queue_url)
