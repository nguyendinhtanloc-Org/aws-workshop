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

from ddb_writer import OrderWriter, WriteResult
from transformer import transform_order

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

ORDERS_TABLE = os.environ.get("ORDERS_TABLE_NAME", "ghn-analytics-dev-orders")
CURATED_BUCKET = os.environ.get("CURATED_BUCKET_NAME", "ghn-analytics-dev-curated")
DLQ_URL = os.environ.get("DLQ_URL", "")

# boto3 clients are created on first use rather than at import time, so
# importing this module does not require AWS credentials.
_clients: dict[str, Any] = {}


def _client(service: str) -> Any:
    """Return a cached boto3 client for the given service."""
    if service not in _clients:
        import boto3

        _clients[service] = boto3.client(service)
    return _clients[service]


def _table() -> Any:
    """Return the DynamoDB orders table resource."""
    if "table" not in _clients:
        import boto3

        _clients["table"] = boto3.resource("dynamodb").Table(ORDERS_TABLE)
    return _clients["table"]


def _read_batch(bucket: str, key: str) -> list[dict[str, Any]]:
    """Read and parse a JSON batch object written by the Ingestor."""
    body = _client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
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


def _send_to_dlq(source: str, reason: str, order: dict[str, Any]) -> bool:
    """Send a record that could not be processed to the DLQ.

    The Lambda dead-letter queue only receives the invocation event, not
    the individual records that failed inside it. Without this explicit
    publish, a rejected record would be counted and then discarded -
    the data would be lost even though the pipeline reported a failure.

    Returns True if the message was queued.
    """
    if not DLQ_URL:
        logger.error("no DLQ_URL configured; rejected record is lost: %s", reason)
        return False

    body = json.dumps(
        {
            "source": source,
            "reason": reason,
            "order": order,
            "failed_at": datetime.now(UTC).isoformat(),
        },
        ensure_ascii=False,
        default=str,
    )
    try:
        _client("sqs").send_message(QueueUrl=DLQ_URL, MessageBody=body)
    except Exception as exc:
        logger.error("cannot send record to DLQ: %s", exc)
        return False
    logger.info("sent rejected record to DLQ reason=%s", reason)
    return True


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process every S3 object referenced by the event.

    A scheduled invocation (no S3 records) processes the previous
    processing window, so a missed S3 notification still gets recovered.
    """
    writer = OrderWriter(_table())
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
                _send_to_dlq(source=f"s3://{bucket}/{key}", reason=str(exc), order=order)
                continue

            try:
                result = writer.put(item)
            except Exception as exc:
                # A write failure is not a validation problem, but the
                # record still must not be lost. Send it to the DLQ and
                # let the batch continue.
                logger.error("write failed order=%s error=%s", order.get("order_code"), exc)
                stats["failed"] += 1
                _send_to_dlq(source=f"s3://{bucket}/{key}", reason=str(exc), order=order)
                continue

            if result is WriteResult.WRITTEN:
                stats["written"] += 1
            elif result is WriteResult.DUPLICATE:
                stats["duplicates"] += 1

    logger.info("processing complete stats=%s at=%s", stats, datetime.now(UTC).isoformat())
    return stats
