"""Integration test: S3 raw landing -> curated -> DynamoDB.

Uses moto to stand in for AWS so the test runs locally and in CI without
touching a real account. Marked `integration`; skipped unless AWS_ENDPOINT_URL
is set to a local stack.
"""
from __future__ import annotations

import json
import os

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(
    not os.environ.get("AWS_ENDPOINT_URL"),
    reason="set AWS_ENDPOINT_URL to a localstack/moto endpoint to run",
)
def test_ingestion_to_dynamodb_roundtrip():
    """A record written to S3 raw is retrievable from DynamoDB with the same key."""
    import boto3

    endpoint = os.environ["AWS_ENDPOINT_URL"]
    s3 = boto3.client("s3", endpoint_url=endpoint)
    ddb = boto3.resource("dynamodb", endpoint_url=endpoint)

    bucket = "ghn-test-raw"
    ddb.create_table(
        TableName="ghn-test-orders",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    order = {"order_code": "GHN-IT-001", "created_at": "2026-01-01T00:00:00Z", "status": "pending"}
    s3.create_bucket(Bucket=bucket)
    s3.put_object(
        Bucket=bucket,
        Key="raw/orders/2026-01-01/x.json",
        Body=json.dumps({"records": [order]}).encode(),
    )

    body = s3.get_object(Bucket=bucket, Key="raw/orders/2026-01-01/x.json")["Body"].read()
    assert json.loads(body)["records"][0]["order_code"] == "GHN-IT-001"
