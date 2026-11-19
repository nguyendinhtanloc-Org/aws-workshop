"""Processing stack: SQS FIFO events into DynamoDB."""
from __future__ import annotations

from typing import Any

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_lambda_event_sources as event_sources
from aws_cdk import aws_sqs as sqs
from cdk_constructs.lambda_function import PipelineLambdaFunction
from constructs import Construct

from stacks.base_stack import BaseStack


class ProcessingStack(Stack):
    """Validates, transforms and deduplicates records before they land in DDB."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        webhook_queue: sqs.IQueue,
        **props: Any,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- DLQ: records that fail validation or write ------------------
        self.processing_dlq = sqs.Queue(
            self,
            "ProcessingDLQ",
            retention_period=Duration.days(14),
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- Lambda: Processor --------------------------------------------
        self.processor = PipelineLambdaFunction(
            self,
            "Processor",
            code_path="build/lambda-processor",
            timeout=Duration.minutes(2),
            memory_size=1024,
            reserved_concurrent_executions=20,
            dead_letter_queue=self.processing_dlq,
            environment={
                "ORDERS_TABLE_NAME": base.orders_table.table_name,
                "CURATED_BUCKET_NAME": base.curated_bucket.bucket_name,
                "DLQ_URL": self.processing_dlq.queue_url,
            },
            log_retention=base.log_retention,
        )

        # DynamoDB L2 names its data grants with a "_data" suffix.
        base.orders_table.grant_read_write_data(self.processor)
        base.curated_bucket.grant_read(self.processor)
        self.processing_dlq.grant_send_messages(self.processor)
        # One message per invocation lets a failure retry without replaying
        # other FIFO messages. The event source also grants consume permissions.
        self.processor.add_event_source(
            event_sources.SqsEventSource(webhook_queue, batch_size=1)
        )

        # Explicit replay of legacy S3 batches still needs read access.
        base.raw_bucket.grant_read(self.processor)

        CfnOutput(self, "ProcessorFunctionName", value=self.processor.function_name)
        CfnOutput(self, "ProcessingDLQUrl", value=self.processing_dlq.queue_url)
