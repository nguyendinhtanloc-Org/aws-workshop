"""Lambda Ingestor: pull data from the GHN API and land it in S3 raw + Kinesis.

Contract
--------
Input  : EventBridge scheduled event (no payload) or a manual test payload.
Output : S3 objects under raw/<source>/<YYYY-MM-DD>/<timestamp>.json
         and one Kinesis record per order on the orders stream.

The S3 write always happens first. If the Kinesis publish then fails, the
raw object is still on disk and the record is not lost - Firehose delivery
is replayable from raw.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

from ingestor_config import get_config
from ghn_client import GhnClient, GhnClientError
from validator import ValidationError, validate_master_data, validate_order

logger = logging.getLogger()

# boto3 clients are created at import time. In AWS the Lambda runtime
# already provides credentials, so this is safe and cheap.
s3 = boto3.client("s3")
kinesis = boto3.client("kinesis")

# Order matters: master data must land before orders that reference it.
SOURCES = ("master_data", "orders", "shipping_status")


def _get_token() -> str:
    """Read the GHN API token - from Secrets Manager in AWS, .env locally."""
    secret_arn = os.environ.get("GHN_SECRET_ARN")
    if not secret_arn:
        return get_config()["ghn_token"]
    try:
        resp = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    except ClientError as exc:
        raise GhnClientError(f"cannot read secret {secret_arn}: {exc}") from exc
    return json.loads(resp["SecretString"])["token"]


def _write_raw(source: str, payload: list[dict[str, Any]]) -> str:
    """Write one source's records to S3 raw under a date-partitioned key."""
    now = datetime.now(UTC)
    key = f"raw/{source}/{now:%Y-%m-%d}/{now:%Y%m%dT%H%M%S%f}.json"
    body = json.dumps(
        {"source": source, "ingested_at": now.isoformat(), "records": payload},
        ensure_ascii=False,
        default=str,
    )
    s3.put_object(
        Bucket=os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"),
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return key


def _publish(source: str, records: list[dict[str, Any]]) -> int:
    """Publish records to the Kinesis stream. Returns the number sent."""
    stream_name = os.environ.get("KINESIS_STREAM_NAME", "")
    if not stream_name or not records:
        return 0

    sent = 0
    for record in records:
        payload = json.dumps(
            {"source": source, "ingested_at": datetime.now(UTC).isoformat(), "record": record},
            ensure_ascii=False,
            default=str,
        )
        kinesis.put_record(
            StreamName=stream_name,
            Data=payload.encode("utf-8"),
            PartitionKey=str(record.get("order_code") or record.get("entity") or sent),
        )
        sent += 1
    return sent


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Fetch each source, write it to S3 raw, and publish it to Kinesis.

    A failure in one source is logged and counted, but does not stop the
    other sources: partial ingestion is better than none, and the missing
    source is retried on the next scheduled run.
    """
    logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    client = GhnClient(token=_get_token())
    summary: dict[str, Any] = {"written": {}, "published": {}, "errors": {}}

    for source in SOURCES:
        try:
            if source == "master_data":
                records = client.fetch_master_data()
                validate_master_data(records)
            elif source == "orders":
                records = client.fetch_orders()
                for order in records:
                    validate_order(order)
            else:
                records = client.fetch_shipping_status()

            key = _write_raw(source, records)
            summary["written"][source] = {"count": len(records), "key": key}
            logger.info("ingested %s records=%s key=%s", source, len(records), key)
        except (GhnClientError, ValidationError) as exc:
            summary["errors"][source] = str(exc)
            logger.error("ingest failed source=%s error=%s", source, exc)
            continue

        # Raw is already durable. A publish failure is not data loss.
        try:
            published = _publish(source, records)
            summary["published"][source] = published
        except Exception as exc:
            summary["errors"][f"{source}_publish"] = str(exc)
            logger.error("publish failed source=%s error=%s", source, exc)

    return summary
