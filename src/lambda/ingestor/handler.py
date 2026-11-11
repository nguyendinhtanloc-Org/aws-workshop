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

from ghn_client import GhnClient, GhnClientError
from ingestor_config import get_config
from normaliser import normalise_order
from validator import (
    STATUS_NORMALISATION,
    ValidationError,
    validate_master_data,
    validate_order,
)

logger = logging.getLogger()

# Order matters: master data must land before orders that reference it.
SOURCES = ("master_data", "orders", "shipping_status")

# --- Order watchlist ---------------------------------------------------------
# The GHN API cannot list orders, so *which* codes to poll is pipeline
# state, not configuration. It lives in the order_watchlist table
# (created by the CDK base stack) - one row per order code:
#
#   pk = "#WATCH"          sk = order_code
#   attributes: registered_at, updated_at, last_status, terminal (bool)
#
# New codes get rows via scripts/create_demo_order.py and, eventually,
# the GHN order-status webhook receiver. When a poll observes a terminal
# status we flip the row's terminal flag so the next run stops polling
# it (and its API calls stop costing money). GHN_ORDER_CODES remains
# only as a bootstrap seed: codes still sitting in that env var are
# registered in the table on their first scheduled run, after which the
# table is the single source of truth and no redeploy is needed for new
# orders.
WATCH_PK = "#WATCH"


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


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
        return str(get_config()["ghn_token"])
    try:
        resp = _client("secretsmanager").get_secret_value(SecretId=secret_arn)
    except Exception as exc:
        raise GhnClientError(f"cannot read secret {secret_arn}: {exc}") from exc
    return str(json.loads(resp["SecretString"])["token"])


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


def _terminal(status: str) -> bool:
    """True when a GHN status is final: no later detail fetch can move it."""
    normalised = STATUS_NORMALISATION.get(status)
    return normalised in {"delivered", "cancelled", "failed", "returned"}


def _table_client() -> Any:
    return _client("dynamodb")


def _scan_watchlist() -> dict[str, bool]:
    """order_code -> terminal flag, for every row in the watchlist.

    One row per code (pk="#WATCH", sk=order_code). Pagination:
    DynamoDB Scan returns up to 1 MB per page; follow LastEvaluatedKey
    until it is gone.
    """
    table_name = os.environ.get("ORDER_WATCHLIST_TABLE", "")
    if not table_name:
        return {}
    items: list[dict[str, Any]] = []
    exclusive_start_key: dict[str, Any] | None = None
    while True:
        kwargs: dict[str, Any] = {"TableName": table_name, "Limit": 1000}
        if exclusive_start_key:
            kwargs["ExclusiveStartKey"] = exclusive_start_key
        resp = _table_client().scan(**kwargs)
        items.extend(resp.get("Items", []))
        exclusive_start_key = resp.get("LastEvaluatedKey")
        if not exclusive_start_key:
            break
    return {str(item["sk"]): bool(item.get("terminal", False)) for item in items if "sk" in item}


def _load_order_codes() -> list[str]:
    """Poll set = non-terminal watchlist rows, plus env codes not yet in the table.

    GHN_ORDER_CODES is a bootstrap seed: codes still sitting in it get
    registered in the table on their first scheduled run, after which
    the table is the single source of truth and new orders no longer
    need a redeploy of the Ingestor's environment. A code already marked
    terminal in the table is *not* re-activated by a stale env entry -
    terminal means done.
    """
    in_table = _scan_watchlist()
    env_codes = _split_csv(os.environ.get("GHN_ORDER_CODES", ""))
    to_register = [code for code in env_codes if code not in in_table]
    if to_register:
        _register_codes(to_register)
    active = [code for code, terminal in in_table.items() if not terminal]
    return active + to_register


def _register_codes(codes: list[str]) -> None:
    """Register order codes as watchlist rows (idempotent)."""
    table_name = os.environ.get("ORDER_WATCHLIST_TABLE", "")
    if not table_name:
        logger.info("watchlist table not configured; %d code(s) stay env-driven", len(codes))
        return
    now = datetime.now(UTC).isoformat()
    for code in codes:
        _table_client().put_item(
            TableName=table_name,
            Item={
                "pk": WATCH_PK,
                "sk": code,
                "terminal": False,
                "registered_at": now,
                "updated_at": now,
            },
        )
    logger.info("registered %d order code(s) in the watchlist table", len(codes))


def _record_poll_results(poll_codes: list[str], polled_statuses: dict[str, str]) -> None:
    """Persist the latest observed status; mark terminal codes done.

    An order whose status is terminal will not move again, so its row's
    terminal flag is set: the next run stops polling it and its API
    calls stop costing money.
    """
    table_name = os.environ.get("ORDER_WATCHLIST_TABLE", "")
    if not table_name or not poll_codes:
        return
    now = datetime.now(UTC).isoformat()
    for code in poll_codes:
        status = polled_statuses.get(code, "")
        if not status:
            continue
        terminal = _terminal(status)
        _table_client().update_item(
            TableName=table_name,
            Key={"pk": WATCH_PK, "sk": code},
            UpdateExpression="SET last_status = :s, updated_at = :t, terminal = :x",
            ExpressionAttributeValues={
                ":s": status,
                ":t": now,
                ":x": terminal,
            },
        )
        if terminal:
            logger.info("order %s reached terminal status %s; no longer polled", code, status)


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


def _is_mock() -> bool:
    return os.environ.get("GHN_MOCK", "").lower() in ("1", "true", "yes")


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

    order_codes = _load_order_codes()
    ward_district_ids = [int(x) for x in _split_csv(os.environ.get("GHN_WARD_DISTRICTS", ""))]
    _polled_orders: list[dict[str, Any]] = []

    for source in SOURCES:
        try:
            if source == "master_data":
                records = client.fetch_master_data(
                    ward_district_ids=ward_district_ids or None
                )
                validate_master_data(records)
            elif source == "orders":
                raw_records = client.fetch_orders(order_codes=order_codes or None)
                # GHN-native field names become internal ones here; a
                # record the normaliser rejects lands in quarantine
                # below, not in the raw store's good-record set.
                normalised: list[dict[str, Any]] = []
                rejected: list[dict[str, Any]] = []
                for raw in raw_records:
                    try:
                        normalised.append(normalise_order(raw))
                    except ValueError as exc:
                        rejected.append({"record": raw, "reason": str(exc)})
                records = normalised
                _polled_orders = records
                if rejected:
                    quarantine_key = _write_raw(f"quarantine/{source}", rejected)
                    summary.setdefault("quarantined", {})[f"{source}_normalise"] = {
                        "count": len(rejected),
                        "key": quarantine_key,
                    }
                    logger.warning(
                        "quarantined %s unnormalisable order record(s) key=%s",
                        len(rejected),
                        quarantine_key,
                    )
                # Keep the watchlist fresh: record each polled order's
                # current status, and archive the ones that reached a
                # terminal state so the next run stops polling them.
                # Watchlist bookkeeping must never fail the source's own
                # ingest.
                try:
                    _record_poll_results(
                        order_codes,
                        {
                            str(r.get("order_code", "")): str(r.get("status", ""))
                            for r in raw_records
                        },
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("watchlist update failed: %s", exc)
            else:
                # There is no dedicated status endpoint in this API
                # generation: each polled order contributes one
                # current-status record. *Changes* between runs are
                # delivered by GHN's order-status webhook in
                # production - see docs/runbook.md.
                if _is_mock():
                    records = client.fetch_shipping_status()
                else:
                    records = [
                        {
                            "order_code": order.get("order_code"),
                            "status": order.get("status"),
                            "status_time": order.get("created_at"),
                            "note": "current status from polled order detail",
                        }
                        for order in _polled_orders
                    ]
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
