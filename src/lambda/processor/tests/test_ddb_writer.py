"""Tests for the idempotent DynamoDB writer."""
from __future__ import annotations

from typing import Any

from ddb_writer import OrderWriter, WriteResult


class FakeTable:
    """Minimal stand-in for a boto3 DynamoDB Table resource."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict[str, Any]] = {}
        self.put_calls = 0

    def put_item(
        self,
        Item: dict[str, Any],
        ConditionExpression: str,
        ExpressionAttributeValues: dict[str, Any] | None = None,
    ) -> None:
        self.put_calls += 1
        key = (Item["pk"], Item["sk"])
        if key in self.items:
            existing = self.items[key]
            # 1. Terminal immunity
            if existing.get("is_terminal"):
                raise Exception(
                    "An error occurred (ConditionalCheckFailedException) when calling the PutItem operation"
                )
            # 2. Incoming is terminal
            if Item.get("is_terminal"):
                self.items[key] = Item
                return
            # 3. Delivery attempt comparison
            existing_attempt = existing.get("delivery_attempt", 1)
            new_attempt = Item.get("delivery_attempt", 1)
            if new_attempt > existing_attempt:
                self.items[key] = Item
                return
            elif new_attempt < existing_attempt:
                raise Exception(
                    "An error occurred (ConditionalCheckFailedException) when calling the PutItem operation"
                )
            # 4. Same attempt: phase_rank comparison
            existing_phase = existing.get("phase_rank", 0)
            new_phase = Item.get("phase_rank", 0)
            if new_phase > existing_phase:
                self.items[key] = Item
                return
            elif new_phase < existing_phase:
                raise Exception(
                    "An error occurred (ConditionalCheckFailedException) when calling the PutItem operation"
                )
            # 5. Same attempt and same phase_rank: updated_at comparison
            new_updated = Item.get("updated_at")
            existing_updated = existing.get("updated_at")
            if existing_updated is None or (new_updated and new_updated > existing_updated):
                self.items[key] = Item
                return
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
    item = {"pk": "ORDER#A", "sk": "2026-01-01", "updated_at": "2026-01-01T10:00:00Z"}
    writer.put(item)
    result = writer.put(dict(item))
    assert result is WriteResult.DUPLICATE
    assert len(table.items) == 1
    assert table.put_calls == 2


def test_newer_status_update_overwrites_existing_order():
    table = FakeTable()
    writer = OrderWriter(table)
    writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "pending",
            "updated_at": "2026-01-01T10:00:00Z",
        }
    )
    # Subsequent update with newer timestamp
    result = writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivered",
            "updated_at": "2026-01-01T11:00:00Z",
        }
    )
    assert result is WriteResult.WRITTEN
    assert table.items[("ORDER#A", "2026-01-01")]["status"] == "delivered"


def test_out_of_order_stale_update_is_rejected():
    table = FakeTable()
    writer = OrderWriter(table)
    # First delivered status arrives (11:00)
    writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivered",
            "updated_at": "2026-01-01T11:00:00Z",
        }
    )
    # Stale 'in_transit' event arrives later (10:30)
    result = writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "in_transit",
            "updated_at": "2026-01-01T10:30:00Z",
        }
    )
    assert result is WriteResult.DUPLICATE
    # Status remains 'delivered', not corrupted
    assert table.items[("ORDER#A", "2026-01-01")]["status"] == "delivered"


def test_same_code_different_date_is_a_separate_item():
    table = FakeTable()
    writer = OrderWriter(table)
    writer.put({"pk": "ORDER#A", "sk": "2026-01-01"})
    writer.put({"pk": "ORDER#A", "sk": "2026-01-02"})
    assert len(table.items) == 2


def test_retry_loop_second_attempt_succeeds():
    table = FakeTable()
    writer = OrderWriter(table)
    # Attempt 1 delivery fails
    writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivering",
            "delivery_attempt": 1,
            "updated_at": "2026-01-01T10:00:00Z",
        }
    )
    writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivery_fail",
            "delivery_attempt": 1,
            "updated_at": "2026-01-01T11:00:00Z",
        }
    )
    # Attempt 2 starts (redelivery)
    res1 = writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivering",
            "delivery_attempt": 2,
            "updated_at": "2026-01-01T14:00:00Z",
        }
    )
    assert res1 is WriteResult.WRITTEN
    assert table.items[("ORDER#A", "2026-01-01")]["delivery_attempt"] == 2
    assert table.items[("ORDER#A", "2026-01-01")]["status"] == "delivering"

    # Attempt 2 delivers successfully
    res2 = writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivered",
            "is_terminal": True,
            "delivery_attempt": 2,
            "updated_at": "2026-01-01T15:00:00Z",
        }
    )
    assert res2 is WriteResult.WRITTEN
    assert table.items[("ORDER#A", "2026-01-01")]["status"] == "delivered"


def test_terminal_order_rejects_earlier_delivery_fail():
    table = FakeTable()
    writer = OrderWriter(table)
    # Order is already delivered at attempt 2
    writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivered",
            "is_terminal": True,
            "delivery_attempt": 2,
            "updated_at": "2026-01-01T15:00:00Z",
        }
    )
    # Late arriving delivery_fail from attempt 1
    result = writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "delivery_fail",
            "is_terminal": False,
            "delivery_attempt": 1,
            "updated_at": "2026-01-01T11:00:00Z",
        }
    )
    assert result is WriteResult.DUPLICATE
    assert table.items[("ORDER#A", "2026-01-01")]["status"] == "delivered"


def test_same_attempt_lower_phase_rank_rejected_despite_clock_skew():
    table = FakeTable()
    writer = OrderWriter(table)
    # Order reaches in_transit (phase_rank=30) at 10:00
    writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "in_transit",
            "delivery_attempt": 1,
            "phase_rank": 30,
            "updated_at": "2026-01-01T10:00:00Z",
        }
    )
    # Late picking event (phase_rank=20) arrives with clock skew (10:05) in same attempt 1
    result = writer.put(
        {
            "pk": "ORDER#A",
            "sk": "2026-01-01",
            "status": "picked",
            "delivery_attempt": 1,
            "phase_rank": 20,
            "updated_at": "2026-01-01T10:05:00Z",
        }
    )
    assert result is WriteResult.DUPLICATE
    assert table.items[("ORDER#A", "2026-01-01")]["status"] == "in_transit"


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
