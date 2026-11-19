#!/usr/bin/env python3
"""Create a visible fake webhook in local Docker services.

Usage:
    python scripts/demo_webhook.py
    python scripts/demo_webhook.py --status delivered

This creates no GHN shipment and calls no GHN API. It invokes the same
Webhook Lambda handler locally, which writes S3 Raw, Watchlist, and FIFO.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    import boto3
except ImportError:
    raise SystemExit("boto3 is required: python -m pip install boto3") from None

ROOT = Path(__file__).resolve().parents[1]
INGESTOR = ROOT / "src" / "lambda" / "ingestor"
sys.path.insert(0, str(INGESTOR))

import webhook_handler  # noqa: E402

REGION = "ap-southeast-1"
DEFAULT_ENDPOINT = "http://localhost:4566"
DEFAULT_BUCKET = "ghn-analytics-local-raw"
DEFAULT_WATCHLIST = "ghn-analytics-local-watchlist"
DEFAULT_QUEUE = "ghn-analytics-local-webhook.fifo"


def aws_client(service: str, endpoint: str) -> Any:
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--status",
        choices=("ready_to_pick", "picking", "delivering", "delivered"),
        default="delivering",
        help="fake GHN order status (default: delivering)",
    )
    args = parser.parse_args()

    endpoint = os.environ.get("LOCALSTACK_ENDPOINT", DEFAULT_ENDPOINT).rstrip("/")
    bucket = os.environ.get("RAW_BUCKET_NAME", DEFAULT_BUCKET)
    table_name = os.environ.get("ORDER_WATCHLIST_TABLE", DEFAULT_WATCHLIST)
    queue_name = os.environ.get("LOCAL_WEBHOOK_QUEUE_NAME", DEFAULT_QUEUE)
    secret = os.environ.get("GHN_WEBHOOK_SECRET", "local-demo-secret")

    s3 = aws_client("s3", endpoint)
    sqs = aws_client("sqs", endpoint)
    ddb = boto3.resource(
        "dynamodb",
        endpoint_url=endpoint,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    try:
        s3.head_bucket(Bucket=bucket)
        watchlist = ddb.Table(table_name)
        watchlist.table_status
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except Exception as exc:
        raise SystemExit(
            f"Khong thay tai nguyen LocalStack ({exc}). Hay chay truoc:\n"
            "  docker compose -f docker-compose.local.yml up -d\n"
            "  python scripts/init_local_env.py"
        ) from exc

    code = f"DEMO-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    order = {
        "OrderCode": code,
        "ClientOrderCode": code,
        "Type": "switch_status",
        "Status": args.status,
        "CreatedDate": now,
        "Time": now,
        "Weight": 1200,
        "CODAmount": 0,
        "ToName": "Nguoi nhan demo",
        "ToPhone": "0900000000",
        "ToAddress": "Dia chi demo, Quan 1, TP Ho Chi Minh",
        "Description": "Du lieu gia de demo pipeline local",
    }
    body = json.dumps(order, ensure_ascii=False, separators=(",", ":"))
    signature = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()

    os.environ.update(
        {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_DEFAULT_REGION": REGION,
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "RAW_BUCKET_NAME": bucket,
            "ORDER_WATCHLIST_TABLE": table_name,
            "WEBHOOK_SQS_URL": queue_url,
            "GHN_WEBHOOK_SECRET": secret,
        }
    )
    for key in ("GHN_WEBHOOK_SECRET_ARN",):
        os.environ.pop(key, None)
    webhook_handler._clients = {
        "s3": s3,
        "sqs": sqs,
        "dynamodb": boto3.client(
            "dynamodb",
            endpoint_url=endpoint,
            region_name=REGION,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        ),
    }

    response = webhook_handler.lambda_handler(
        {"headers": {"x-ghn-signature": signature}, "body": body}, None
    )
    if response["statusCode"] != 200:
        raise SystemExit(f"Webhook handler failed: {response}")

    item = watchlist.get_item(
        Key={"pk": "#WATCH", "sk": code}, ConsistentRead=True
    ).get("Item")
    objects = s3.list_objects_v2(
        Bucket=bucket, Prefix="raw/webhook/"
    ).get("Contents", [])
    raw_key = next(
        (obj["Key"] for obj in objects if f"/{code}_" in obj["Key"]), None
    )
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages"],
    )["Attributes"]

    print("Webhook gia da duoc xu ly.")
    print(f"Ma don mau: {code}")
    print(f"Trang thai mau: {args.status}")
    print(f"Watchlist: {json.dumps(item, ensure_ascii=False, default=str)}")
    print(f"S3 Raw: s3://{bucket}/{raw_key}" if raw_key else "S3 Raw: KHONG TIM THAY")
    print(f"SQS FIFO: {queue_name} ({attributes.get('ApproximateNumberOfMessages', '0')} message dang cho, uoc tinh)")
    print("Day la du lieu gia local; chua tao don tren GHN.")
    if not item or not raw_key:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
