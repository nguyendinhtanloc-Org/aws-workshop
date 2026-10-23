"""Tests for the idempotent DynamoDB writer."""
from __future__ import annotations

from typing import Any

from ddb_writer import OrderWriter, WriteResult


class FakeTable:
    """Minimal stand-in for a boto3 DynamoDB Table resource."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.put_calls = 0

    def put_item(self, Item: dict[str, Any], ConditionExpression: str) -> None:
        self.put_calls += 1
        key = (Item["pk"], Item["sk"])
        if key in self.items:
            raise Exception(
                "An error occurred (ConditionalCheckFailedException) when calling the PutItem operation"
            )
        self.items[key] = Item


def test_first_write_succeeds():
    table = FakeTable()
    result = OrderWriter(table).put({"pk": "ORDER#A", "sk": "2026-01-01"})
    assert result is WriteResult.WRITTEN
    assert len(table.items) == 1


def test_repeated_write_is_reported_as_duplicate():
    table = FakeTable()
    writer = OrderWriter(table)
    item = {"pk": "ORDER#A", "sk": "2026-01-01"}
    writer.put(item)
    result = writer.put(dict(item))
    assert result is WriteResult.DUPLICATE
    assert len(table.items) == 1
    assert table.put_calls == 2


def test_same_code_different_date_is_a_separate_item():
    table = FakeTable()
    writer = OrderWriter(table)
    writer.put({"pk": "ORDER#A", "sk": "2026-01-01"})
    writer.put({"pk": "ORDER#A", "sk": "2026-01-02"})
    assert len(table.items) == 2


def test_other_errors_are_not_swallowed():
    class BrokenTable:
        def put_item(self, Item: dict[str, Any], ConditionExpression: str) -> None:
            raise RuntimeError("ProvisionedThroughputExceededException")

    try:
        OrderWriter(BrokenTable()).put({"pk": "p", "sk": "s"})
    except RuntimeError as exc:
        assert "ProvisionedThroughput" in str(exc)
    else:
        raise AssertionError("expected RuntimeError to propagate")
