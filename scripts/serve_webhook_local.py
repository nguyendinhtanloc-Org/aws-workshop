#!/usr/bin/env python3
"""Receive GHN staging callbacks on loopback and forward to webhook_handler.

Run after LocalStack initialization: python scripts/serve_webhook_local.py
"""
from __future__ import annotations

import base64
import contextlib
import json
import logging
import os
import sys
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import boto3

with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "lambda" / "ingestor"))
import webhook_handler  # noqa: E402

MAX_BODY_BYTES = 1024 * 1024
REGION = "ap-southeast-1"
STAGING_URL = "https://dev-online-gateway.ghn.vn"


def configure_localstack() -> None:
    """Fail at startup when credentials, destination or LocalStack is missing."""
    if os.environ.get("GHN_BASE_URL", "").rstrip("/") != STAGING_URL:
        raise RuntimeError("GHN_BASE_URL must be the staging gateway")
    token = os.environ.get("GHN_API_TOKEN", "")
    shop_id = os.environ.get("GHN_SHOP_ID", "")
    if not token or token == "your_ghn_api_token_here" or not shop_id.strip():
        raise RuntimeError("GHN_API_TOKEN and GHN_SHOP_ID are required for order detail")
    callback_token = os.environ.get("GHN_CALLBACK_SHARED_TOKEN", "")
    if len(callback_token) < 24 or callback_token.startswith("replace_"):
        raise RuntimeError("GHN_CALLBACK_SHARED_TOKEN must be a private random value")
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    if not endpoint.startswith(("http://localhost:", "http://127.0.0.1:")):
        raise RuntimeError("LOCALSTACK_ENDPOINT must point to local loopback")
    os.environ.update({
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_DEFAULT_REGION": REGION,
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "GHN_WEBHOOK_AUTH_MODE": "shared_token",
        "GHN_WEBHOOK_ENRICH_FROM_DETAIL": "1",
        "RAW_BUCKET_NAME": os.environ.get("LOCAL_RAW_BUCKET_NAME", "ghn-analytics-local-raw"),
        "ORDER_WATCHLIST_TABLE": os.environ.get("LOCAL_WATCHLIST_TABLE_NAME", "ghn-analytics-local-watchlist"),
    })
    os.environ.pop("GHN_WEBHOOK_SECRET_ARN", None)
    sqs = boto3.client("sqs", endpoint_url=endpoint, region_name=REGION)
    queue_name = os.environ.get("LOCAL_WEBHOOK_QUEUE_NAME", "ghn-analytics-local-webhook.fifo")
    os.environ["WEBHOOK_SQS_URL"] = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    boto3.client("s3", endpoint_url=endpoint, region_name=REGION).head_bucket(Bucket=os.environ["RAW_BUCKET_NAME"])
    boto3.client("dynamodb", endpoint_url=endpoint, region_name=REGION).describe_table(
        TableName=os.environ["ORDER_WATCHLIST_TABLE"]
    )
    webhook_handler._clients.clear()
    webhook_handler._clients.update({
        "s3": boto3.client("s3", endpoint_url=endpoint, region_name=REGION),
        "sqs": sqs,
    })


class WebhookServer(ThreadingHTTPServer):
    daemon_threads = True


class Receiver(BaseHTTPRequestHandler):
    """Only the GHN webhook route is exposed through the tunnel."""

    def _reply(self, status: int, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path != "/health":
            self._reply(404, '{"error":"not found"}')
        else:
            self._reply(200, '{"status":"ok"}')

    def do_POST(self) -> None:
        if self.path != "/webhook/ghn":
            self._reply(404, '{"error":"not found"}')
            return
        length_text = self.headers.get("Content-Length", "")
        try:
            length = int(length_text)
        except ValueError:
            self._reply(411, '{"error":"Content-Length required"}')
            return
        if length < 1:
            self._reply(400, '{"error":"empty body"}')
            return
        if length > MAX_BODY_BYTES:
            self._reply(413, '{"error":"request too large"}')
            return
        raw = self.rfile.read(length)
        if len(raw) != length:
            self._reply(400, '{"error":"incomplete body"}')
            return
        event = {
            "headers": dict(self.headers.items()),
            "body": base64.b64encode(raw).decode("ascii"),
            "isBase64Encoded": True,
        }
        response = webhook_handler.lambda_handler(event, None)
        try:
            payload = json.loads(raw)
            data = payload.get("data", payload) if isinstance(payload, dict) else {}
            code = data.get("OrderCode") or data.get("order_code") or "?"
            kind = data.get("Type") or data.get("event_type") or "?"
        except (UnicodeDecodeError, json.JSONDecodeError):
            code, kind = "?", "?"
        logging.info("time=%s order_code=%s type=%s result=%s", datetime.now(UTC).isoformat(), code, kind, response["statusCode"])
        self._reply(response["statusCode"], response["body"])

    def log_message(self, format: str, *args: object) -> None:
        # The default server logger includes paths but should never print headers.
        return


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        configure_localstack()
    except Exception as exc:
        print(f"Startup failed: {type(exc).__name__}. Check staging settings and run init_local_env.py.", file=sys.stderr)
        return 1
    port = int(os.environ.get("LOCAL_WEBHOOK_PORT", "8000"))
    with WebhookServer(("127.0.0.1", port), Receiver) as server:
        print(f"GHN staging webhook receiver listening on http://127.0.0.1:{port}/webhook/ghn")
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
