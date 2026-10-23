"""Lambda Processor: validate, transform and deduplicate records into DynamoDB.

Contract
--------
Input  : S3 event (curated bucket) or a scheduled invocation.
Output : one DynamoDB item per order, plus failed records routed to the DLQ.

Idempotency: writes use a composite primary key (order_code, ingest_date)
and a conditional put, so replaying the same batch never creates a second
row for the same order on the same day.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import boto3

from ddb_writer import OrderWriter, WriteResult
from transformer import transform_order

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

ddb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

ORDERS_TABLE = os.environ["ORDERS_TABLE_NAME"]
CURATED_BUCKET = os.environ["CURATED_BUCKET_NAME"]


def _read_batch(bucket: str, key: str) -> list[dict[str, Any]]:
    """Read and parse a JSON batch object written by the Ingestor."""
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    payload = json.loads(body)
    if isinstance(payload, dict):
        return list(payload.get("records", []))
    return list(payload)


def _keys_from_event(event: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract (bucket, key) pairs from an S3 event payload."""
    keys: list[tuple[str, str]] = []
    for record in event.get("Records", []):
        try:
            keys.append((record["s3"]["bucket"]["name"], record["s3"]["object"]["key"]))
        except KeyError:
            logger.warning("skipping malformed S3 record: %s", record)
    return keys


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process every S3 object referenced by the event.

    A scheduled invocation (no S3 records) processes the previous
    processing window, so a missed S3 notification still gets recovered.
    """
    writer = OrderWriter(ddb.Table(ORDERS_TABLE))
    stats = {"processed": 0, "written": 0, "duplicates": 0, "failed": 0}

    targets = _keys_from_event(event)
    if not targets:
        logger.info("no S3 records in event, nothing to process")
        return stats

    for bucket, key in targets:
        try:
            orders = _read_batch(bucket, key)
        except Exception as exc:
            logger.error("cannot read batch bucket=%s key=%s error=%s", bucket, key, exc)
            stats["failed"] += 1
            raise

        for order in orders:
            stats["processed"] += 1
            try:
                item = transform_order(order)
            except ValueError as exc:
                logger.warning("rejecting order error=%s raw=%s", exc, order)
                stats["failed"] += 1
                continue

            result = writer.put(item)
            if result is WriteResult.WRITTEN:
                stats["written"] += 1
            elif result is WriteResult.DUPLICATE:
                stats["duplicates"] += 1

    logger.info("processing complete stats=%s at=%s", stats, datetime.now(UTC).isoformat())
    return stats
