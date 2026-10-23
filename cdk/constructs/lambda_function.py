"""Reusable construct for the project's Lambda functions.

Centralises the settings every function in this project must have: ARM
architecture, X-Ray tracing, log retention, and the retry/DLQ pair. Using
one construct means a change to these defaults lands in every function at
once, and there is no per-function drift.
"""
from __future__ import annotations

from aws_cdk import Duration
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sqs as sqs
from constructs import Construct


class PipelineLambdaFunction(lambda_.Function):
    """A Lambda function with the project's standard configuration applied."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        code_path: str,
        handler: str = "handler.lambda_handler",
        log_retention: logs.RetentionDays = logs.RetentionDays.ONE_MONTH,
        timeout: Duration = Duration.minutes(2),
        memory_size: int = 512,
        reserved_concurrent_executions: int = 2,
        dead_letter_queue: sqs.IQueue | None = None,
        environment: dict[str, str] | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler=handler,
            code=lambda_.Code.from_asset(code_path),
            timeout=timeout,
            memory_size=memory_size,
            reserved_concurrent_executions=reserved_concurrent_executions,
            tracing=lambda_.Tracing.ACTIVE,
            log_retention=log_retention,
            retry_attempts=2,
            dead_letter_queue=dead_letter_queue,
            environment=environment or {},
            **kwargs,  # type: ignore[arg-type]
        )
