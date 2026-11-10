"""GHN Webhook Receiver Lambda Handler.

Handles incoming Webhook events pushed by GHN (e.g. create, switch_status, update_weight).
Verifies HMAC signature, writes raw event to S3 Raw, and routes to SQS FIFO queue.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

from normaliser import normalise_order

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_clients: dict[str, Any] = {}


def _client(service: str) -> Any:
    if service not in _clients:
        import boto3

        _clients[service] = boto3.client(service)
    return _clients[service]


def verify_signature(body: bytes, signature: str | None, secret: str) -> bool:
    """Verify incoming HMAC-SHA256 signature from GHN."""
    if not secret:
        # If no secret configured (local dev/testing), permit
        return True
    if not signature:
        return False
    computed = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed.lower(), signature.strip().lower())


def parse_webhook_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize a GHN Webhook callback payload.

    GHN Webhook payload usually carries:
    - OrderCode / order_code
    - Status / status
    - Time / updated_date / created_date
    - Type / event_type (e.g. "switch_status")
    """
    if not isinstance(payload, dict):
        raise ValueError("webhook payload must be a JSON object")

    data = dict(payload.get("data") if "data" in payload and isinstance(payload["data"], dict) else payload)

    # Standardize casing across create, switch_status, and update_weight callbacks
    for pascal, snake in (
        ("OrderCode", "order_code"),
        ("ClientOrderCode", "client_order_code"),
        ("Status", "status"),
        ("Time", "updated_date"),
        ("CreatedDate", "created_date"),
        ("FinishDate", "finish_date"),
        ("Type", "event_type"),
        ("TotalFee", "shipping_fee"),
        ("CODAmount", "cod_amount"),
        ("Weight", "weight"),
        ("ConvertedWeight", "converted_weight"),
        ("Length", "length"),
        ("Width", "width"),
        ("Height", "height"),
        ("ToName", "to_name"),
        ("ToPhone", "to_phone"),
        ("ToAddress", "to_address"),
        ("ToProvince", "to_province"),
        ("Description", "description"),
        ("Warehouse", "warehouse"),
    ):
        if pascal in data:
            data.setdefault(snake, data.pop(pascal))

    # For update_weight events that may omit Status, infer from log or default to storing
    if not data.get("status") and str(data.get("event_type", "")).lower() == "update_weight":
        log_entries = data.get("log")
        if isinstance(log_entries, list) and log_entries and isinstance(log_entries[-1], dict):
            data["status"] = log_entries[-1].get("status") or "storing"
        else:
            data["status"] = "storing"

    # Fallback created_date if missing: prefer first log entry timestamp to keep partition key stable
    if "created_date" not in data and "created_at" not in data:
        log_entries = data.get("log")
        first_log_ts = (
            log_entries[0].get("updated_date")
            if isinstance(log_entries, list) and log_entries and isinstance(log_entries[0], dict)
            else None
        )
        data["created_date"] = first_log_ts or data.get("updated_date") or datetime.now(UTC).isoformat()

    return normalise_order(data)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """API Gateway Lambda proxy integration handler for GHN Webhooks."""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    secret = os.environ.get("GHN_WEBHOOK_SECRET", "")
    signature = headers.get("x-ghn-signature") or headers.get("signature")

    raw_body = event.get("body", "")
    is_base64 = event.get("isBase64Encoded", False)
    body_bytes = base64.b64decode(raw_body) if is_base64 else (raw_body.encode("utf-8") if isinstance(raw_body, str) else b"")

    if not verify_signature(body_bytes, signature, secret):
        logger.warning("invalid webhook signature received")
        return {"statusCode": 401, "body": json.dumps({"error": "invalid signature"})}

    try:
        payload = json.loads(body_bytes.decode("utf-8") if body_bytes else "{}")
        order_record = parse_webhook_payload(payload)
    except Exception as exc:
        logger.error("failed to parse webhook payload: %s", exc)
        return {"statusCode": 400, "body": json.dumps({"error": str(exc)})}

    # 1. Write Raw backup
    now = datetime.now(UTC)
    raw_bucket = os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw")
    code = order_record.get("order_code", "UNKNOWN")
    raw_key = f"raw/webhook/{now:%Y-%m-%d}/{code}_{now:%H%M%S%f}.json"
    try:
        _client("s3").put_object(
            Bucket=raw_bucket,
            Key=raw_key,
            Body=body_bytes,
            ContentType="application/json",
        )
    except Exception as exc:
        logger.warning("could not write raw webhook backup: %s", exc)

    # 2. Push to SQS FIFO queue if configured
    sqs_url = os.environ.get("WEBHOOK_SQS_URL", "")
    if sqs_url:
        try:
            _client("sqs").send_message(
                QueueUrl=sqs_url,
                MessageBody=json.dumps(order_record, ensure_ascii=False, default=str),
                MessageGroupId=code,
                MessageDeduplicationId=f"{code}_{order_record.get('status')}_{now.timestamp()}",
            )
        except Exception as exc:
            logger.error("failed to forward webhook to SQS: %s", exc)
            return {"statusCode": 500, "body": json.dumps({"error": "queue delivery failed"})}

    logger.info("webhook processed successfully for order_code=%s status=%s", code, order_record.get("status"))
    return {"statusCode": 200, "body": json.dumps({"status": "ok", "order_code": code})}

