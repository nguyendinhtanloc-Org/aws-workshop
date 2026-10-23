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
