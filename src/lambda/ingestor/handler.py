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

from ingestor_config import get_config
from ghn_client import GhnClient, GhnClientError
from validator import ValidationError, validate_master_data, validate_order

logger = logging.getLogger()

# Order matters: master data must land before orders that reference it.
SOURCES = ("master_data", "orders", "shipping_status")

# boto3 clients are created on first use rather than at import time.
# Importing this module must not require AWS credentials or open a
# connection - that is what keeps the unit tests and local scripts free
# of an AWS account.
_clients: dict[str, Any] = {}


def _client(service: str) -> Any:
    """Return a cached boto3 client for the given service.

    boto3 is imported here rather than at module scope so that importing
    this module does not require the AWS SDK - which is what lets the
    unit tests run without it.
    """
    if service not in _clients:
        import boto3

        _clients[service] = boto3.client(service)
    return _clients[service]


def _get_token() -> str:
    """Read the GHN API token - from Secrets Manager in AWS, .env locally."""
    secret_arn = os.environ.get("GHN_SECRET_ARN")
    if not secret_arn:
        return get_config()["ghn_token"]
    try:
        resp = _client("secretsmanager").get_secret_value(SecretId=secret_arn)
    except Exception as exc:
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
    _client("s3").put_object(
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
        _client("kinesis").put_record(
            StreamName=stream_name,
            Data=payload.encode("utf-8"),
            PartitionKey=str(record.get("order_code") or record.get("entity") or sent),
        )
        sent += 1
    return sent


def _partition_valid(
    source: str, records: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split records into (valid, rejected).

    Only the orders source is record-validated; master data and shipping
    status are validated as a whole.
    """
    if source != "orders":
        return records, []

    good: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for order in records:
        try:
            validate_order(order)
        except ValidationError as exc:
            rejected.append({"record": order, "reason": str(exc)})
        else:
            good.append(order)
    return good, rejected


def _make_client() -> Any:
    """Build the API client: the mock when GHN_MOCK is set, else the real one."""
    if os.environ.get("GHN_MOCK", "").lower() in ("1", "true", "yes"):
        from mock_source import MockGhnClient

        logger.info("using mock GHN data source")
        return MockGhnClient()

    config = get_config()
    shop_id = config.get("ghn_shop_id", "")
    if not shop_id:
        logger.warning(
            "GHN_SHOP_ID is not set; the current gateway requires it "
            "alongside the token and may reject every request"
        )
    return GhnClient(
        token=config["ghn_token"],
        base_url=config["ghn_base_url"],
        shop_id=shop_id,
    )


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Fetch each source, write it to S3 raw, and publish it to Kinesis.

    A failure in one source is logged and counted, but does not stop the
    other sources: partial ingestion is better than none, and the missing
    source is retried on the next scheduled run.
    """
    logging.getLogger().setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    client = _make_client()
    summary: dict[str, Any] = {"written": {}, "published": {}, "errors": {}}

    for source in SOURCES:
        try:
            if source == "master_data":
                records = client.fetch_master_data()
                validate_master_data(records)
            elif source == "orders":
                records = client.fetch_orders()
            else:
                records = client.fetch_shipping_status()
        except (GhnClientError, ValidationError) as exc:
            # A source-level failure (API down, master data incomplete)
            # affects every record in it, so the whole source is skipped
            # and retried on the next scheduled run.
            summary["errors"][source] = str(exc)
            logger.error("ingest failed source=%s error=%s", source, exc)
            continue

        # A single bad record must not cost us the whole batch. Records
        # that fail validation are written to a quarantine prefix and
        # counted, and the good ones continue.
        good, rejected = _partition_valid(source, records)
        if rejected:
            quarantine_key = _write_raw(f"quarantine/{source}", rejected)
            summary.setdefault("quarantined", {})[source] = {
                "count": len(rejected),
                "key": quarantine_key,
            }
            logger.warning(
                "quarantined %s invalid record(s) source=%s key=%s",
                len(rejected),
                source,
                quarantine_key,
            )

        key = _write_raw(source, good)
        summary["written"][source] = {"count": len(good), "key": key}
        logger.info("ingested %s records=%s key=%s", source, len(good), key)

        # Raw is already durable. A publish failure is not data loss.
        try:
            published = _publish(source, good)
            summary["published"][source] = published
        except Exception as exc:
            summary["errors"][f"{source}_publish"] = str(exc)
            logger.error("publish failed source=%s error=%s", source, exc)

    return summary
