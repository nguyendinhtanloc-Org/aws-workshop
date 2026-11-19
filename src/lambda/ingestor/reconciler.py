"""Daily snapshot reconciliation."""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ingestor_config import make_client
from normaliser import normalise_order
from watchlist import is_terminal


class LeaseLost(RuntimeError):
    pass


def business_hash(order: dict[str, Any]) -> str:
    fields = (
        "order_code",
        "status",
        "updated_at",
        "created_at",
        "delivered_at",
        "weight",
        "shipping_fee",
        "cod_amount",
        "log",
        "to_name",
        "to_phone",
        "to_address",
        "to_province_id",
        "to_district_id",
    )
    payload = {k: order.get(k) for k in fields}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    ).hexdigest()


def open_orders(table: Any, page_size: int = 100) -> Iterator[str]:
    from boto3.dynamodb.conditions import Key

    args = {
        "KeyConditionExpression": Key("pk").eq("#WATCH"),
        "ConsistentRead": True,
        "Limit": page_size,
    }
    while True:
        response = table.query(**args)
        for row in response.get("Items", []):
            if not row.get("terminal", False):
                yield row["sk"]
        if not response.get("LastEvaluatedKey"):
            break
        args["ExclusiveStartKey"] = response["LastEvaluatedKey"]


def acquire(
    table: Any, code: str, owner: str, now: int, seconds: int = 120
) -> dict[str, Any] | None:
    from botocore.exceptions import ClientError

    try:
        lease: dict[str, Any] = table.update_item(
            Key={"pk": "#RECON", "sk": code},
            UpdateExpression="SET owner_id=:o, expires_at=:e ADD fencing_token :one",
            ConditionExpression="attribute_not_exists(expires_at) OR expires_at < :now",
            ExpressionAttributeValues={":o": owner, ":e": now + seconds, ":now": now, ":one": 1},
            ReturnValues="ALL_NEW",
        )["Attributes"]
        return lease
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return None
        raise


def fenced_update(
    table: Any,
    code: str,
    lease: dict[str, Any],
    now: int,
    digest: str | None = None,
    release: bool = False,
) -> None:
    values = {":o": lease["owner_id"], ":f": lease["fencing_token"], ":now": now}
    expression = "SET checked_at=:now"
    if digest is not None:
        expression += ", batch_hash=:h"
        values[":h"] = digest
    if release:
        expression += ", expires_at=:zero"
        values[":zero"] = 0
    table.update_item(
        Key={"pk": "#RECON", "sk": code},
        UpdateExpression=expression,
        ConditionExpression="owner_id=:o AND fencing_token=:f AND expires_at >= :now",
        ExpressionAttributeValues=values,
    )


def serving_matches(table: Any, order: dict[str, Any]) -> bool:
    from boto3.dynamodb.conditions import Key

    args = {
        "KeyConditionExpression": Key("pk").eq("ORDER#" + order["order_code"]),
        "ConsistentRead": True,
    }
    items = []
    while True:
        response = table.query(**args)
        items.extend(response.get("Items", []))
        if not response.get("LastEvaluatedKey"):
            break
        args["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    from validator import STATUS_NORMALISATION

    wanted = STATUS_NORMALISATION.get(order["status"], order["status"])

    def ts(value: Any) -> datetime:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    for row in items:
        if row.get("status") != wanted:
            continue
        if not row.get("updated_at") or not order.get("updated_at"):
            continue
        if ts(row["updated_at"]) < ts(order["updated_at"]):
            continue
        if all(
            row.get(k)
            == (Decimal(str(order[k])) if isinstance(order.get(k), float) else order.get(k))
            for k in ("weight", "shipping_fee", "cod_amount")
            if k in order
        ):
            return True
    return False


def reconcile_one(
    code: str,
    api: Any,
    watch: Any,
    state: Any,
    serving: Any,
    s3: Any,
    sqs: Any,
    clock: Any = time.time,
) -> str:
    owner = uuid.uuid4().hex
    lease = acquire(state, code, owner, int(clock()))
    if lease is None:
        return "locked"
    try:
        records = api.fetch_orders(order_codes=[code])
        ingest_time = datetime.fromtimestamp(clock(), UTC).isoformat()
        if len(records) != 1:
            raise ValueError("expected one GHN order snapshot")
        order = normalise_order(records[0])
        if order["order_code"] != code:
            raise ValueError("GHN returned a different order code")
        digest = business_hash(order)
        matched = serving_matches(serving, order)
        if lease.get("batch_hash") == digest and matched:
            result = "unchanged"
        else:
            fenced_update(state, code, lease, int(clock()))
            body = json.dumps(
                {"source": "orders", "ingested_at": ingest_time, "records": [order]}, default=str
            ).encode()
            s3.put_object(
                Bucket=os.environ["RAW_BUCKET_NAME"],
                Key=f"raw/reconciliation/{code}/{owner}.json",
                Body=body,
                ContentType="application/json",
            )
            fenced_update(state, code, lease, int(clock()))
            sqs.send_message(
                QueueUrl=os.environ["WEBHOOK_SQS_URL"],
                MessageBody=json.dumps(order, default=str),
                MessageAttributes={
                    "source": {"DataType": "String", "StringValue": "ghn_reconciliation"},
                    "ingest_time": {"DataType": "String", "StringValue": ingest_time},
                },
                MessageGroupId=code,
                MessageDeduplicationId=digest,
            )
            result = "sent"
        fenced_update(state, code, lease, int(clock()), digest=digest)
        if matched and is_terminal(order["status"]):
            watch.update_item(
                Key={"pk": "#WATCH", "sk": code},
                UpdateExpression="SET terminal=:t, last_status=:s",
                ExpressionAttributeValues={":t": True, ":s": order["status"]},
            )
        return result
    finally:
        from botocore.exceptions import ClientError

        try:
            fenced_update(state, code, lease, int(clock()), release=True)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    import boto3

    db = boto3.resource("dynamodb")
    watch = db.Table(os.environ["ORDER_WATCHLIST_TABLE"])
    state = db.Table(os.environ["RECON_STATE_TABLE"])
    serving = db.Table(os.environ["ORDERS_TABLE_NAME"])
    api = make_client()
    stats = {"sent": 0, "unchanged": 0, "locked": 0, "failed": 0}
    for code in open_orders(watch):
        if context and context.get_remaining_time_in_millis() < 130000:
            raise RuntimeError(
                "reconciliation exceeded invocation budget; remaining orders need a retry"
            )
        try:
            result = reconcile_one(
                code, api, watch, state, serving, boto3.client("s3"), boto3.client("sqs")
            )
            stats[result] += 1
        except Exception:
            import logging

            logging.getLogger(__name__).exception("reconciliation failed for %s", code)
            stats["failed"] += 1
    if stats["failed"]:
        raise RuntimeError(f"reconciliation failures: {stats}")
    return stats
