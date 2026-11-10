"""Tests for the order transformation rules."""
from __future__ import annotations

import pytest

from transformer import transform_order

BASE = {
    "order_code": "GHN001",
    "created_at": "2026-01-01T00:00:00Z",
    "status": "delivered",
    "delivered_at": "2026-01-01T06:00:00Z",
}


def test_composite_key_is_built_from_code_and_date():
    item = transform_order(dict(BASE))
    assert item["pk"] == "ORDER#GHN001"
    assert item["sk"] == "2026-01-01"


def test_lead_time_is_computed_from_timestamps():
    item = transform_order(dict(BASE))
    assert item["lead_time_hours"] == 6.0


def test_lead_time_is_none_when_not_delivered():
    order = dict(BASE)
    del order["delivered_at"]
    order["status"] = "in_transit"
    item = transform_order(order)
    assert item["lead_time_hours"] is None


def test_naive_timestamp_is_treated_as_utc():
    order = dict(BASE, created_at="2026-01-01T00:00:00", delivered_at="2026-01-01T06:00:00")
    item = transform_order(order)
    assert item["lead_time_hours"] == 6.0


def test_missing_order_code_raises():
    order = dict(BASE)
    del order["order_code"]
    with pytest.raises(ValueError, match="order_code"):
        transform_order(order)


def test_unparseable_created_at_raises():
    order = dict(BASE, created_at="not-a-date")
    with pytest.raises(ValueError, match="unparseable created_at"):
        transform_order(order)


def test_optional_fields_default_to_none():
    order = {"order_code": "G2", "created_at": "2026-01-01T00:00:00Z", "status": "pending"}
    item = transform_order(order)
    assert item["weight"] is None
    assert item["delivered_at"] is None


def test_numeric_fields_are_decimal():
    """boto3 rejects Python floats, so numeric fields must be Decimal."""
    from decimal import Decimal

    order = dict(BASE, weight=2.5, shipping_fee=32000, cod_amount=150000.0)
    item = transform_order(order)
    assert isinstance(item["weight"], Decimal)
    assert isinstance(item["shipping_fee"], Decimal)
    assert isinstance(item["cod_amount"], Decimal)
    assert isinstance(item["lead_time_hours"], Decimal)


def test_ghn_status_is_normalised():
    """GHN's raw status values map onto the project's vocabulary."""
    cases = {
        "ready_to_pick": "pending",
        "picking": "picked",
        "storing": "in_transit",
        "transporting": "in_transit",
        "delivering": "in_transit",
        "delivered": "delivered",
        "delivery_fail": "failed",
        "return": "returned",
        "returning": "returned",
        "cancel": "cancelled",
        "lost": "failed",
    }
    for raw, expected in cases.items():
        item = transform_order(dict(BASE, status=raw))
        assert item["status"] == expected, raw


def test_unknown_status_passes_through_unchanged():
    """The validator rejects unknown statuses; the transformer does not guess."""
    item = transform_order(dict(BASE, status="something_new"))
    assert item["status"] == "something_new"
    assert item["is_terminal"] is False
    assert item["phase_rank"] == 0


def test_seven_terminal_states_marked_terminal():
    """All 7 GHN terminal states must have is_terminal=True; non-terminal states False."""
    seven_terminals = (
        "delivered",
        "returned",
        "cancel",
        "exception",
        "lost",
        "damage",
        "scrap",
    )
    for raw in seven_terminals:
        item = transform_order(dict(BASE, status=raw))
        assert item["is_terminal"] is True, f"Expected {raw} to be terminal"

    for non_term in ("ready_to_pick", "picking", "storing", "delivering", "delivery_fail", "returning"):
        item = transform_order(dict(BASE, status=non_term))
        assert item["is_terminal"] is False, f"Expected {non_term} to be non-terminal"


def test_delivery_attempt_computed_from_log():
    """delivery_attempt = 1 + prior delivery_fail occurrences in log."""
    # First delivery failure itself is still attempt 1
    order_fail_1 = dict(
        BASE,
        status="delivery_fail",
        updated_at="2026-01-01T11:00:00Z",
        log=[
            {"status": "delivering", "updated_date": "2026-01-01T09:00:00Z"},
            {"status": "delivery_fail", "updated_date": "2026-01-01T11:00:00Z"},
        ],
    )
    assert transform_order(order_fail_1)["delivery_attempt"] == 1

    # Redelivery after 1 prior fail gets delivery_attempt = 2
    order_retry_2 = dict(
        BASE,
        status="delivering",
        updated_at="2026-01-01T14:00:00Z",
        log=[
            {"status": "delivering", "updated_date": "2026-01-01T09:00:00Z"},
            {"status": "delivery_fail", "updated_date": "2026-01-01T11:00:00Z"},
            {"status": "delivering", "updated_date": "2026-01-01T14:00:00Z"},
        ],
    )
    item_retry_2 = transform_order(order_retry_2)
    assert item_retry_2["delivery_attempt"] == 2
    assert item_retry_2["phase_rank"] == 40

