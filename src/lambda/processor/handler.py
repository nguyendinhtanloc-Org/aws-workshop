"""Lambda Processor: validate, transform and deduplicate records into DynamoDB.

Contract
--------
Input  : S3 event or SQS event containing a normalized order.
Output : one DynamoDB item per order, plus failed records routed to the DLQ.

Idempotency: writes use a composite primary key (order_code, ingest_date)
and a conditional put, so replaying the same batch never creates a second
row for the same order on the same day.
"""
from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import unquote_plus

from bronze import write_order
from ddb_writer import OrderWriter, WriteResult
from transformer import transform_order

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

ORDERS_TABLE = os.environ.get("ORDERS_TABLE_NAME", "ghn-analytics-dev-orders")
DLQ_URL = os.environ.get("DLQ_URL", "")

# boto3 clients are created on first use rather than at import time, so
# importing this module does not require AWS credentials.
_clients: dict[str, Any] = {}


def _client(service: str) -> Any:
    """Return a cached boto3 client for the given service."""
    if service not in _clients:
        import boto3

        _clients[service] = cast(Any, boto3).client(service)
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
        if "records" in payload and isinstance(payload["records"], list):
            return list(payload["records"])
        if "order_code" in payload or "OrderCode" in payload:
            return [payload]
        return []
    return list(payload)


def _keys_from_event(event: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract (bucket, key) pairs from an S3 event payload (direct or SQS-wrapped)."""
    keys: list[tuple[str, str]] = []
    for record in event.get("Records", []):
        if "s3" in record:
            try:
                bucket = record["s3"]["bucket"]["name"]
                key = unquote_plus(record["s3"]["object"]["key"])
                keys.append((bucket, key))
            except KeyError:
                logger.warning("skipping malformed S3 record: %s", record)
        elif "body" in record:
            try:
                inner = json.loads(record["body"])
            except (ValueError, TypeError):
                continue
            if isinstance(inner, dict) and "Records" in inner:
                keys.extend(_keys_from_event(inner))
    return keys


def _direct_orders_from_event(
    event: dict[str, Any],
) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    """Extract direct order payloads delivered via SQS (e.g. from webhook FIFO queue)."""
    direct: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for record in event.get("Records", []):
        if "s3" in record or "body" not in record:
            continue
        try:
            body = json.loads(record["body"])
        except (ValueError, TypeError):
            if record.get("eventSource") == "aws:sqs":
                raise ValueError("SQS message body must contain valid JSON") from None
            logger.warning("skipping non-JSON SQS record")
            continue
        if isinstance(body, dict) and "Records" not in body and "order_code" in body:
            source = str(record.get("eventSourceARN") or record.get("messageId") or "sqs")
            direct.append((source, body, record))
        elif record.get("eventSource") == "aws:sqs" and (
            not isinstance(body, dict) or not _keys_from_event(body)
        ):
            raise ValueError("SQS message must contain an order or an S3 notification")
    return direct


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
    """Process S3 batches and SQS orders; SQS handles retries and redrive."""
    from_sqs = any(record.get("eventSource") == "aws:sqs" for record in event.get("Records", []))
    writer = OrderWriter(_table())
    stats = {"processed": 0, "written": 0, "duplicates": 0, "failed": 0}

    targets = _keys_from_event(event)
    direct_orders = _direct_orders_from_event(event)
    if not targets and not direct_orders:
        logger.info("no S3 or SQS order records in event, nothing to process")
        return stats

    bronze_bucket = os.environ.get("MINIO_BRONZE_BUCKET")
    batches: list[tuple[str, list[dict[str, Any]], dict[str, Any] | None]] = []
    for bucket, key in targets:
        try:
            orders = _read_batch(bucket, key)
        except Exception as exc:
            logger.error("cannot read batch bucket=%s key=%s error=%s", bucket, key, exc)
            stats["failed"] += 1
            raise
        batches.append((f"s3://{bucket}/{key}", orders, None))

    for source, order, message in direct_orders:
        batches.append((source, [order], message))

    for source, orders, message in batches:
        for order in orders:
            stats["processed"] += 1
            try:
                item = transform_order(order)
            except ValueError as exc:
                if from_sqs:
                    raise
                logger.warning("rejecting order error=%s raw=%s", exc, order)
                stats["failed"] += 1
                _send_to_dlq(source=source, reason=str(exc), order=order)
                continue

            try:
                if bronze_bucket and message is not None:
                    # Acknowledge only after both sinks succeed. Both writes can be replayed.
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        archive = executor.submit(write_order, bronze_bucket, order, message)
                        result = writer.put(item)
                        archive.result()
                else:
                    result = writer.put(item)
            except Exception as exc:
                if from_sqs:
                    raise
                # A write failure is not a validation problem, but the
                # record still must not be lost. Send it to the DLQ and
                # let the batch continue.
                logger.error("write failed order=%s error=%s", order.get("order_code"), exc)
                stats["failed"] += 1
                _send_to_dlq(source=source, reason=str(exc), order=order)
                continue

            if result is WriteResult.WRITTEN:
                stats["written"] += 1
            elif result is WriteResult.DUPLICATE:
                stats["duplicates"] += 1

    logger.info("processing complete stats=%s at=%s", stats, datetime.now(UTC).isoformat())
    return stats
