"""Reusable construct for the project's Lambda functions.

Centralises the settings every function in this project must have: ARM
architecture, X-Ray tracing, log retention, and the retry/DLQ pair. Using
one construct means a change to these defaults lands in every function at
once, and there is no per-function drift.
"""
from __future__ import annotations

from pathlib import Path

from aws_cdk import Duration
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sqs as sqs
from constructs import Construct

# code_path is passed relative to the *repo root* (e.g. "build/lambda-ingestor",
# where scripts/build_lambdas.py writes). The CDK app is executed with a CWD of
# its context dir (where cdk.json lives, i.e. cdk/), NOT the repo root, so a
# naive CWD-relative lookup would look in the wrong place. Anchor every asset
# to the repo root - this file is cdk/cdk_constructs/lambda_function.py, so the
# root is three parents up - to make the path deterministic regardless of CWD.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


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
        asset_path = _REPO_ROOT / code_path
        if not asset_path.is_dir():
            raise RuntimeError(
                f"Lambda code dir {code_path!r} not found "
                f"(looked at {asset_path}). Run "
                f"`python3 scripts/build_lambdas.py` before cdk deploy."
            )
        super().__init__(
            scope,
            construct_id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            architecture=lambda_.Architecture.ARM_64,
            handler=handler,
            # code_path must be a *built* package (build/lambda-*),
            # produced by scripts/build_lambdas.py: the Lambda runtime
            # has no `requests`, so an unbundled source dir fails on the
            # very first import.
            code=lambda_.Code.from_asset(
                str(asset_path),
                exclude=["tests", "__pycache__", "*.pyc"],
            ),
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
