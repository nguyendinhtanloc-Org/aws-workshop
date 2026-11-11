"""Processing stack: S3 event -> Lambda Processor -> DynamoDB, with SQS DLQ."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack, StackProps
# aws_s3_notifications (not the older aws_lambda_destinations module,
# whose cross-stack LambdaDestination crashes jsii with
# "reading 'toString'" when the destination Lambda lives in a
# different stack than the notification bucket).
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sqs as sqs
from constructs import Construct

from cdk_constructs.lambda_function import PipelineLambdaFunction
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
            code_path="build/lambda-processor",
            timeout=Duration.minutes(2),
            memory_size=1024,
            reserved_concurrent_executions=5,
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

        # --- Trigger: S3 notification on the raw bucket -------------------
        # Processing is event-driven: a new raw order object triggers a run.
        # Only the orders prefix is wired, so master data batches do not
        # trigger the Processor.
        #
        # The notification is attached to a *name-imported* reference to the
        # raw bucket, not to base.raw_bucket itself. The bucket lives in the
        # base stack while this Processor lives here; calling
        # add_event_notification on the base-constructed bucket would put the
        # notification's custom resource in the base stack, which then has to
        # reference this stack's Lambda ARN - a base -> processing
        # dependency that collides with processing -> base and makes synth
        # fail with a cyclic reference. Importing by physical name keeps the
        # whole notification (custom resource + Lambda permission) in this
        # stack; the ordering is already guaranteed by
        # processing.add_dependency(base) in app.py.
        raw = s3.Bucket.from_bucket_name(
            self,
            "RawBucket",
            base.raw_bucket.bucket_name,
        )
        raw.grant_read(self.processor)
        # Object-key filters are positional NotificationKeyFilter objects,
        # not a keyword argument (this CDK version's signature is
        # add_event_notification(event, destination, *filters)).
        raw.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.processor),
            s3.NotificationKeyFilter(prefix="raw/orders/"),
        )

        CfnOutput(self, "ProcessorFunctionName", value=self.processor.function_name)
        CfnOutput(self, "ProcessingDLQUrl", value=self.processing_dlq.queue_url)
