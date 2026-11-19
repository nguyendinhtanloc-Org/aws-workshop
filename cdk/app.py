"""CDK application entry point for GHN Logistics Analytics Platform."""
from __future__ import annotations

import contextlib
import os
import subprocess
import sys

from aws_cdk import App, Environment, Tags
from stacks.analytics_stack import AnalyticsStack
from stacks.base_stack import BaseStack
from stacks.ingestion_stack import IngestionStack
from stacks.monitoring_stack import MonitoringStack
from stacks.processing_stack import ProcessingStack

with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv()

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAMBDA_PACKAGES = ("build/lambda-ingestor", "build/lambda-processor")


def _ensure_lambda_packages() -> None:
    missing = [p for p in _LAMBDA_PACKAGES if not os.path.isdir(os.path.join(_REPO_ROOT, p))]
    if not missing:
        return
    print(f"build packages missing ({', '.join(missing)}) - running scripts/build_lambdas.py ...")
    subprocess.run(
        [sys.executable, os.path.join(_REPO_ROOT, "scripts", "build_lambdas.py")],
        check=True,
        cwd=_REPO_ROOT,
    )


_ensure_lambda_packages()

app = App()

env = Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "ap-southeast-1"),
)

project = app.node.try_get_context("project") or "ghn-analytics"
env_name = app.node.try_get_context("env") or "dev"

tags = Tags.of(app)
tags.add("Project", project)
tags.add("Environment", env_name)
tags.add("ManagedBy", "cdk")
tags.add("Owner", "de-team")

minimal = "--minimal" in sys.argv[1:]

base = BaseStack(app, f"{project}-{env_name}-base", with_vpc=not minimal, env=env, description="Shared S3 buckets, KMS, IAM (+ VPC with the analytics stack)")

ingestion = IngestionStack(
    app, f"{project}-{env_name}-ingestion", env=env, base=base,
    description="GHN API ingestion: Lambda + EventBridge + S3 raw",
)

processing = ProcessingStack(
    app, f"{project}-{env_name}-processing", env=env, base=base,
    webhook_queue=ingestion.webhook_queue,
    description="Lambda processor + DynamoDB + SQS DLQ",
)

ingestion.add_dependency(base)
processing.add_dependency(base)

if not minimal:
    analytics = AnalyticsStack(
        app, f"{project}-{env_name}-analytics", env=env, base=base,
        description="Glue Data Catalog + ETL + Athena workgroup",
    )
    monitoring = MonitoringStack(
        app, f"{project}-{env_name}-monitoring", env=env, base=base,
        ingestion=ingestion, processing=processing,
        description="CloudWatch dashboards, alarms, SNS alerts, budget",
    )
    analytics.add_dependency(base)
    monitoring.add_dependency(base)

app.synth()
