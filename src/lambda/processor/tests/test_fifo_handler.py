"""SQS failures must surface to the event source so it can retry the message,
while stale and duplicate records are acknowledged without sending to DLQ.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest


def _get_processor() -> Any:
    # Explicitly load processor handler to avoid collision with ingestor handler
    handler_path = Path(__file__).resolve().parent.parent / "handler.py"
    pkg_dir = str(handler_path.parent)
    repo_src = str(handler_path.parent.parent.parent)
    if pkg_dir not in sys.path:
        sys.path.insert(0, pkg_dir)
    if repo_src not in sys.path:
        sys.path.insert(0, repo_src)

    spec = importlib.util.spec_from_file_location("processor_handler_module", str(handler_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load processor handler from {handler_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_fifo_failures_are_not_acknowledged() -> None:
    cases = [
        ("{invalid", ValueError),
        ('{"status": "delivered"}', ValueError),
        ('{"order_code": "GHN-1"}', ValueError),
        (
            json.dumps({
                "order_code": "GHN-1",
                "status": "delivered",
                "created_at": "2026-11-16T08:00:00Z",
            }),
            RuntimeError,
        ),
    ]
    processor = _get_processor()
    table = Mock()
    table.put_item.side_effect = RuntimeError("DynamoDB unavailable")
    processor._clients["table"] = table

    for body, error in cases:
        event: dict[str, Any] = {"Records": [{"eventSource": "aws:sqs", "body": body}]}
        with pytest.raises(error):
            processor.lambda_handler(event, None)


def test_duplicate_and_stale_records_do_not_route_to_dlq() -> None:
    """Stale/duplicate writes report DUPLICATE (No-op) and do not increase DLQ count."""
    processor = _get_processor()
    table = Mock()
    sqs_mock = Mock()
    processor._clients["table"] = table
    processor._clients["sqs"] = sqs_mock
    processor.DLQ_URL = "https://sqs.mock/dlq"

    # Simulate conditional check failure (duplicate or stale order)
    table.put_item.side_effect = Exception(
        "An error occurred (ConditionalCheckFailedException) when calling PutItem"
    )

    event = {
        "Records": [
            {
                "eventSource": "aws:sqs",
                "body": json.dumps({
                    "order_code": "GHN-TEST-DUP-01",
                    "status": "delivering",
                    "created_at": "2026-01-01T08:00:00Z",
                }),
            }
        ]
    }

    res = processor.lambda_handler(event, None)

    assert res["processed"] == 1
    assert res["written"] == 0
    assert res["duplicates"] == 1
    assert res["failed"] == 0
    assert not sqs_mock.send_message.called, "Stale record must not send messages to DLQ"


def test_malformed_batch_records_route_to_dlq() -> None:
    """Non-SQS batch with malformed payload routes rejected items to DLQ."""
    processor = _get_processor()
    table = Mock()
    sqs_mock = Mock()
    processor._clients["table"] = table
    processor._clients["sqs"] = sqs_mock
    processor.DLQ_URL = "https://sqs.mock/dlq"

    malformed_order = {"status": "delivering", "created_at": "2026-01-01T08:00:00Z"}
    with patch.object(processor, "_read_batch", return_value=[malformed_order]):
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": "raw-bucket"},
                        "object": {"key": "raw/orders/batch_malformed.json"},
                    }
                }
            ]
        }
        res = processor.lambda_handler(event, None)

    assert res["processed"] == 1
    assert res["failed"] == 1
    assert sqs_mock.send_message.called, "Malformed batch record must be sent to DLQ"
    call_args = sqs_mock.send_message.call_args[1]
    body = json.loads(call_args["MessageBody"])
    assert "order_code" in body["reason"]
    assert body["order"] == malformed_order


def test_valid_order_written_successfully() -> None:
    """A valid order is transformed and written to DynamoDB."""
    processor = _get_processor()
    table = Mock()
    table.put_item.side_effect = None
    processor._clients["table"] = table

    event = {
        "Records": [
            {
                "eventSource": "aws:sqs",
                "body": json.dumps({
                    "order_code": "GHN-TEST-VALID-01",
                    "status": "ready_to_pick",
                    "created_at": "2026-01-01T08:00:00Z",
                }),
            }
        ]
    }

    res = processor.lambda_handler(event, None)

    assert res["processed"] == 1
    assert res["written"] == 1
    assert res["duplicates"] == 0
    assert res["failed"] == 0
    assert table.put_item.called
