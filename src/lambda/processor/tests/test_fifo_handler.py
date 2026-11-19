"""SQS failures must surface to the event source so it can retry the message."""
import json
from importlib import import_module
from typing import Any
from unittest.mock import Mock

import pytest


@pytest.mark.parametrize(
    ("body", "error"),
    [
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
    ],
)
def test_fifo_failures_are_not_acknowledged(
    monkeypatch: pytest.MonkeyPatch, body: str, error: type[Exception]
) -> None:
    table = Mock()
    table.put_item.side_effect = RuntimeError("DynamoDB unavailable")
    processor = import_module("src.lambda.processor.handler")
    monkeypatch.setitem(processor._clients, "table", table)

    event: dict[str, Any] = {"Records": [{"eventSource": "aws:sqs", "body": body}]}
    with pytest.raises(error):
        processor.lambda_handler(event, None)
