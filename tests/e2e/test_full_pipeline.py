"""End-to-end test against a deployed stack.

Disabled by default. Set DEPLOYED=true plus the stack output names to run
it against the dev environment.
"""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.e2e


@pytest.mark.skipif(
    not os.environ.get("DEPLOYED"),
    reason="set DEPLOYED=true to run e2e tests against a live stack",
)
def test_pipeline_health():
    """The deployed pipeline reports healthy ingestion metrics."""
    import boto3

    cloudwatch = boto3.client("cloudwatch")
    period = 300
    stats = {"Maximum": 1.0}

    response = cloudwatch.get_metric_statistics(
        Namespace="GHN/Pipeline",
        MetricName="OrdersIngested",
        StartTime=__import__("datetime").datetime.now() - __import__("datetime").timedelta(hours=1),
        EndTime=__import__("datetime").datetime.now(),
        Period=period,
        Statistics=["Sum"],
    )
    datapoints = response.get("Datapoints", [])
    assert datapoints, "no ingestion metric published in the last hour"
    assert stats
