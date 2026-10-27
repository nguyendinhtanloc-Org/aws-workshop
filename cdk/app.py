#!/usr/bin/env python3
"""CDK application entry point for GHN Logistics Analytics Platform."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

# .env holds the local developer configuration (staging GHN base URL,
# watchlists, shop id). Real shell exports always win - dotenv only
# fills in names that are not already set - so CI and explicit
# `export GHN_...` usage keep full control.
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from aws_cdk import App, Environment, Tags

from stacks.base_stack import BaseStack
from stacks.ingestion_stack import IngestionStack
from stacks.streaming_stack import StreamingStack
from stacks.processing_stack import ProcessingStack
from stacks.analytics_stack import AnalyticsStack
from stacks.monitoring_stack import MonitoringStack

app = App()

env = Environment(
    account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
    region=os.environ.get("CDK_DEFAULT_REGION", "ap-southeast-1"),
)

# --- Shared context -------------------------------------------------------
project = app.node.try_get_context("project") or "ghn-analytics"
env_name = app.node.try_get_context("env") or "dev"

tags = Tags.of(app)
tags.add("Project", project)
tags.add("Environment", env_name)
tags.add("ManagedBy", "cdk")
tags.add("Owner", "de-team")

# --- Stacks (dependency order) -------------------------------------------
base = BaseStack(app, f"{project}-{env_name}-base", env=env, description="Shared VPC, S3 buckets, KMS, IAM")

# --minimal: deploy the ingest path only (base + streaming + ingestion +
# processing - S3, Kinesis, the two Lambdas, DynamoDB). Skip the Glue/
# Athena analytics stack and the monitoring stack: cheapest way to
# prove the pipeline end to end; deploy them later with a full run.
minimal = "--minimal" in sys.argv[1:]

streaming = StreamingStack(
    app, f"{project}-{env_name}-streaming", env=env, base=base,
    description="Kinesis Data Streams + Firehose buffering + S3 curated",
)

ingestion = IngestionStack(
    app, f"{project}-{env_name}-ingestion", env=env, base=base, streaming=streaming,
    description="GHN API ingestion: Lambda + EventBridge + S3 raw + Kinesis publish",
)

processing = ProcessingStack(
    app, f"{project}-{env_name}-processing", env=env, base=base,
    description="Lambda processor + DynamoDB + SQS DLQ",
)

streaming.add_dependency(base)
ingestion.add_dependency(base)
ingestion.add_dependency(streaming)
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
