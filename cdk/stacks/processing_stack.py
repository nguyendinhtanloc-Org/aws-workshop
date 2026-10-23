"""Processing stack: S3 event -> Lambda Processor -> DynamoDB, with SQS DLQ."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack, StackProps
from aws_cdk import aws_lambda_destinations as destinations
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sqs as sqs
from constructs import Construct

from constructs.lambda_function import PipelineLambdaFunction
from stacks.base_stack import BaseStack


class ProcessingStack(Stack):
    """Validates, transforms and deduplicates records before they land in DDB."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        **props: StackProps,
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
            code_path="src/lambda/processor",
            timeout=Duration.minutes(2),
            memory_size=1024,
            reserved_concurrent_executions=5,
            dead_letter_queue=self.processing_dlq,
            environment={
                "ORDERS_TABLE_NAME": base.orders_table.table_name,
                "CURATED_BUCKET_NAME": base.curated_bucket.bucket_name,
            },
            log_retention=base.log_retention,
        )

        base.orders_table.grant_read_write(self.processor)
        base.raw_bucket.grant_read_write(self.processor)
        base.curated_bucket.grant_read_write(self.processor)

        # --- Trigger: S3 notification on the raw bucket -------------------
        # Processing is event-driven: a new raw order object triggers a run.
        # Only the orders prefix is wired, so master data batches do not
        # trigger the Processor.
        base.raw_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            destinations.LambdaDestination(self.processor),
            prefix="raw/orders/",
        )

        CfnOutput(self, "ProcessorFunctionName", value=self.processor.function_name)
        CfnOutput(self, "ProcessingDLQUrl", value=self.processing_dlq.queue_url)
