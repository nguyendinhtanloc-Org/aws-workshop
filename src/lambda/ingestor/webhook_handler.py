"""GHN webhook handler shared by API Gateway and the local HTTP receiver."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any, cast

from normaliser import normalise_order

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_clients: dict[str, Any] = {}


def _client(service: str) -> Any:
    if service not in _clients:
        import boto3

        _clients[service] = cast(Any, boto3).client(service)
    return _clients[service]


def verify_signature(body: bytes, signature: str | None, secret: str) -> bool:
    """Verify the optional HMAC simulator contract, not a GHN guarantee."""
    if not secret:
        return False
    if not signature:
        return False
    computed = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed.lower(), signature.strip().lower())


def parse_webhook_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize a GHN Webhook callback payload."""
    if not isinstance(payload, dict):
        raise ValueError("webhook payload must be a JSON object")

    raw_data = payload.get("data")
    data: dict[str, Any] = dict(raw_data) if isinstance(raw_data, dict) else dict(payload)

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

    if not data.get("status") and str(data.get("event_type", "")).lower() == "update_weight":
        log_entries = data.get("log")
        if isinstance(log_entries, list) and log_entries and isinstance(log_entries[-1], dict):
            data["status"] = log_entries[-1].get("status") or "storing"
        else:
            data["status"] = "storing"

    return normalise_order(data)


def _enrich_created_date(payload: dict[str, Any]) -> dict[str, Any]:
    """Use GHN order detail; callback Time is the event time, not creation time."""
    nested = payload.get("data")
    data = nested if isinstance(nested, dict) else payload
    if data.get("CreatedDate") or data.get("created_date") or data.get("created_at"):
        return payload
    code = data.get("OrderCode") or data.get("order_code")
    if not code:
        raise ValueError("missing OrderCode")
    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    base = os.environ.get("GHN_BASE_URL", "")
    if not token or not shop or base.rstrip("/") != "https://dev-online-gateway.ghn.vn":
        raise RuntimeError("staging order-detail credentials are not configured")
    from ghn_client import GhnClient

    records = GhnClient(token=token, base_url=base, shop_id=shop).fetch_orders(order_codes=[str(code)])
    if not records or not records[0].get("created_date"):
        raise RuntimeError("order detail has no created_date")
    enriched = dict(payload)
    if data is payload:
        enriched["created_date"] = records[0]["created_date"]
    else:
        enriched["data"] = {**data, "created_date": records[0]["created_date"]}
    return enriched


def _authorized(headers: dict[str, str], body: bytes) -> tuple[bool, bool]:
    """Return (configured, authorized)."""
    mode = os.environ.get("GHN_WEBHOOK_AUTH_MODE", "hmac").lower()
    if mode == "shared_token":
        secret = os.environ.get("GHN_CALLBACK_SHARED_TOKEN", "")
        if not secret:
            return False, False
        provided = headers.get("x-ghn-webhook-token", "")
        return True, bool(provided) and hmac.compare_digest(provided, secret)
    if mode == "hmac":
        secret = os.environ.get("GHN_WEBHOOK_SECRET", "")
        if os.environ.get("GHN_WEBHOOK_SECRET_ARN"):
            secret = _client("secretsmanager").get_secret_value(
                SecretId=os.environ["GHN_WEBHOOK_SECRET_ARN"]
            )["SecretString"]
        if not secret:
            return False, False
        return True, verify_signature(body, headers.get("x-ghn-signature") or headers.get("signature"), secret)
    return False, False


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """API Gateway Lambda proxy integration handler for GHN Webhooks."""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    raw_body = event.get("body", "")
    is_base64 = event.get("isBase64Encoded", False)
    try:
        body_bytes = base64.b64decode(raw_body, validate=True) if is_base64 else (
            raw_body.encode("utf-8") if isinstance(raw_body, str) else b""
        )
    except (ValueError, TypeError):
        return {"statusCode": 400, "body": json.dumps({"error": "invalid base64 body"})}
    try:
        configured, authorized = _authorized(headers, body_bytes)
    except Exception:
        logger.exception("webhook authentication configuration failed")
        return {"statusCode": 503, "body": json.dumps({"error": "authentication unavailable"})}
    if not configured:
        logger.error("webhook authentication is not configured")
        return {"statusCode": 503, "body": json.dumps({"error": "authentication not configured"})}
    if not authorized:
        logger.warning("webhook authentication failed")
        return {"statusCode": 401, "body": json.dumps({"error": "unauthorized"})}

    try:
        payload = json.loads(body_bytes.decode("utf-8") if body_bytes else "{}")
        if not isinstance(payload, dict):
            raise ValueError("webhook payload must be a JSON object")
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return {"statusCode": 400, "body": json.dumps({"error": "invalid JSON object"})}
    try:
        payload_for_normalisation = _enrich_created_date(payload) if os.environ.get("GHN_WEBHOOK_ENRICH_FROM_DETAIL") == "1" else payload
        order_record = parse_webhook_payload(payload_for_normalisation)
    except ValueError as exc:
        logger.warning("invalid webhook order: %s", exc)
        return {"statusCode": 400, "body": json.dumps({"error": str(exc)})}
    except Exception:
        logger.exception("order-detail enrichment failed")
        return {"statusCode": 503, "body": json.dumps({"error": "order detail unavailable"})}

    try:
        from watchlist import register_order
        register_order(str(order_record["order_code"]))
    except Exception:
        logger.exception("watchlist registration failed")
        return {"statusCode": 500, "body": json.dumps({"error": "watchlist unavailable"})}

    now = datetime.now(UTC)
    raw_bucket = os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw")
    code = str(order_record.get("order_code", "UNKNOWN"))
    raw_key = f"raw/webhook/{now:%Y-%m-%d}/{code}_{now:%H%M%S%f}.json"
    envelope = json.dumps(
        {"source": "orders", "ingested_at": now.isoformat(), "records": [order_record]},
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    try:
        _client("s3").put_object(
            Bucket=raw_bucket,
            Key=raw_key,
            Body=envelope,
            ContentType="application/json",
        )
    except Exception as exc:
        logger.warning("could not write raw webhook backup: %s", exc)
        return {"statusCode": 500, "body": json.dumps({"error": "raw backup failed"})}

    sqs_url = os.environ.get("WEBHOOK_SQS_URL", "")
    if sqs_url:
        dedup_raw = (
            f"{code}|{order_record.get('status')}|{order_record.get('updated_at')}"
            f"|{order_record.get('weight')}|{order_record.get('cod_amount')}"
        )
        dedup_id = hashlib.sha256(dedup_raw.encode("utf-8")).hexdigest()
        try:
            _client("sqs").send_message(
                QueueUrl=sqs_url,
                MessageBody=json.dumps(order_record, ensure_ascii=False, default=str),
                MessageGroupId=code,
                MessageDeduplicationId=dedup_id,
                MessageAttributes={
                    "source": {"DataType": "String", "StringValue": "ghn_webhook"},
                    "ingest_time": {"DataType": "String", "StringValue": now.isoformat()},
                },
            )
        except Exception as exc:
            logger.error("failed to forward webhook to SQS: %s", exc)
            return {"statusCode": 500, "body": json.dumps({"error": "queue delivery failed"})}

    logger.info("webhook processed order_code=%s type=%s result=ok", code, order_record.get("event_type"))
    return {"statusCode": 200, "body": json.dumps({"status": "ok", "order_code": code})}

