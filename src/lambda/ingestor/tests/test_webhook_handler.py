"""Tests for the GHN Webhook Receiver handler."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import webhook_handler
from webhook_handler import parse_webhook_payload, verify_signature


def test_verify_signature_valid():
    secret = "test-secret-123"
    body = b'{"OrderCode": "ABC12345"}'
    sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(body, sig, secret) is True


def test_verify_signature_invalid():
    secret = "test-secret-123"
    body = b'{"OrderCode": "ABC12345"}'
    assert verify_signature(body, "wrong-signature", secret) is False


def test_verify_signature_missing():
    secret = "test-secret-123"
    body = b'{"OrderCode": "ABC12345"}'
    assert verify_signature(body, None, secret) is False


def test_verify_signature_missing_secret_fails_closed():
    assert verify_signature(b"{}", "any", "") is False


def test_shared_token_auth_valid_invalid_missing(monkeypatch):
    monkeypatch.setenv("GHN_WEBHOOK_AUTH_MODE", "shared_token")
    monkeypatch.setenv("GHN_CALLBACK_SHARED_TOKEN", "private")
    assert webhook_handler._authorized({"x-ghn-webhook-token": "private"}, b"{}") == (True, True)
    assert webhook_handler._authorized({"x-ghn-webhook-token": "wrong"}, b"{}") == (True, False)
    assert webhook_handler._authorized({}, b"{}") == (True, False)
    monkeypatch.delenv("GHN_CALLBACK_SHARED_TOKEN")
    assert webhook_handler._authorized({}, b"{}") == (False, False)


def test_missing_secret_and_bad_base64_return_error(monkeypatch):
    monkeypatch.setenv("GHN_WEBHOOK_AUTH_MODE", "shared_token")
    monkeypatch.delenv("GHN_CALLBACK_SHARED_TOKEN", raising=False)
    assert webhook_handler.lambda_handler({"headers": {}, "body": "{}"}, None)["statusCode"] == 503
    monkeypatch.setenv("GHN_CALLBACK_SHARED_TOKEN", "private")
    assert webhook_handler.lambda_handler({"headers": {}, "body": "%%%", "isBase64Encoded": True}, None)["statusCode"] == 400


def test_created_date_never_uses_callback_time(monkeypatch):
    monkeypatch.setenv("GHN_BASE_URL", "https://dev-online-gateway.ghn.vn")
    monkeypatch.setenv("GHN_API_TOKEN", "placeholder")
    monkeypatch.setenv("GHN_SHOP_ID", "123")
    callback = {"OrderCode": "GHN123", "Type": "create", "Time": "2026-11-17T04:49:47Z", "Status": "ready_to_pick"}
    with patch("ghn_client.GhnClient.fetch_orders", return_value=[{"created_date": "2026-11-16T08:00:00Z"}]) as detail:
        enriched = webhook_handler._enrich_created_date(callback)
    assert callback.get("created_date") is None
    assert parse_webhook_payload(enriched)["created_at"] == "2026-11-16T08:00:00Z"
    detail.assert_called_once_with(order_codes=["GHN123"])
    with pytest.raises(ValueError, match="created_at"):
        parse_webhook_payload(callback)


def test_parse_webhook_payload_pascal_case():
    payload = {
        "OrderCode": "GHN9988",
        "Status": "delivering",
        "Type": "switch_status",
        "Time": "2026-11-10T08:00:00Z",
        "CreatedDate": "2026-11-09T22:00:00Z",
        "TotalFee": 32000,
        "CODAmount": 250000,
        "ToName": "Nguyen Van A",
        "ToPhone": "0901234567",
        "log": [
            {"status": "ready_to_pick", "updated_date": "2026-11-09T22:00:00Z"},
            {"status": "delivering", "updated_date": "2026-11-10T08:00:00Z"},
        ],
    }
    parsed = parse_webhook_payload(payload)
    assert parsed["order_code"] == "GHN9988"
    assert parsed["status"] == "delivering"
    assert parsed["updated_at"] == "2026-11-10T08:00:00Z"
    assert parsed["created_at"] == "2026-11-09T22:00:00Z"
    assert parsed["to_name"] == "Nguyen Van A"
    assert parsed["to_phone"] == "0901234567"


def test_parse_webhook_payload_update_weight():
    payload = {
        "OrderCode": "GHN9989",
        "Type": "update_weight",
        "Time": "2026-11-10T09:00:00Z",
        "CreatedDate": "2026-11-09T22:00:00Z",
        "Weight": 1500,
        "ConvertedWeight": 1800,
    }
    parsed = parse_webhook_payload(payload)
    assert parsed["order_code"] == "GHN9989"
    assert parsed["status"] == "storing"
    assert parsed["weight"] == 1500
    assert parsed["converted_weight"] == 1800


def test_lambda_handler_success():
    s3_puts: list[dict] = []
    sqs_messages: list[dict] = []

    class FakeS3:
        def put_object(self, **kwargs):
            s3_puts.append(kwargs)
            return {}

    class FakeSQS:
        def send_message(self, **kwargs):
            sqs_messages.append(kwargs)
            return {"MessageId": "msg-123"}

    saved_clients = webhook_handler._clients
    saved_secret = os.environ.get("GHN_WEBHOOK_SECRET")
    saved_url = os.environ.get("WEBHOOK_SQS_URL")
    try:
        webhook_handler._clients = {"s3": FakeS3(), "sqs": FakeSQS()}
        os.environ["GHN_WEBHOOK_SECRET"] = "mysecret"
        os.environ["WEBHOOK_SQS_URL"] = "https://sqs/webhook.fifo"

        body = json.dumps({"OrderCode": "ORDER1", "Status": "delivering", "CreatedDate": "2026-11-09T22:00:00Z"}).encode()
        sig = hmac.new(b"mysecret", body, hashlib.sha256).hexdigest()

        event = {
            "headers": {"x-ghn-signature": sig},
            "body": body.decode(),
        }
        resp = webhook_handler.lambda_handler(event, None)
        assert resp["statusCode"] == 200
        assert "ORDER1" in resp["body"]
        assert len(s3_puts) == 1
        assert len(sqs_messages) == 1
        assert sqs_messages[0]["MessageGroupId"] == "ORDER1"
    finally:
        webhook_handler._clients = saved_clients
        if saved_secret is None:
            os.environ.pop("GHN_WEBHOOK_SECRET", None)
        else:
            os.environ["GHN_WEBHOOK_SECRET"] = saved_secret
        if saved_url is None:
            os.environ.pop("WEBHOOK_SQS_URL", None)
        else:
            os.environ["WEBHOOK_SQS_URL"] = saved_url


def test_lambda_handler_unauthorized():
    saved_clients = webhook_handler._clients
    saved_secret = os.environ.get("GHN_WEBHOOK_SECRET")
    try:
        webhook_handler._clients = {}
        os.environ["GHN_WEBHOOK_SECRET"] = "mysecret"
        event = {
            "headers": {"x-ghn-signature": "bad-sig"},
            "body": '{"OrderCode": "ORDER1"}',
        }
        resp = webhook_handler.lambda_handler(event, None)
        assert resp["statusCode"] == 401
    finally:
        webhook_handler._clients = saved_clients
        if saved_secret is None:
            os.environ.pop("GHN_WEBHOOK_SECRET", None)
        else:
            os.environ["GHN_WEBHOOK_SECRET"] = saved_secret
