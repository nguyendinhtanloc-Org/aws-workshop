"""Tests for the ingest-side schema validation."""
from __future__ import annotations

import pytest

from validator import ValidationError, validate_master_data, validate_order

VALID_ORDER = {
    "order_id": "123",
    "order_code": "GHN001",
    "from_province_id": 1,
    "to_province_id": 2,
    "status": "pending",
    "created_at": "2026-01-01T00:00:00Z",
    "weight": 1.5,
}


def test_valid_order_passes():
    validate_order(dict(VALID_ORDER))


def test_missing_required_field_raises():
    order = dict(VALID_ORDER)
    del order["order_code"]
    with pytest.raises(ValidationError, match="order_code"):
        validate_order(order)


def test_unknown_status_raises():
    order = dict(VALID_ORDER, status="teleported")
    with pytest.raises(ValidationError, match="unknown status"):
        validate_order(order)


def test_negative_weight_raises():
    order = dict(VALID_ORDER, weight=-3)
    with pytest.raises(ValidationError, match="invalid weight"):
        validate_order(order)


def test_non_dict_raises():
    with pytest.raises(ValidationError, match="must be an object"):
        validate_order(["not", "a", "dict"])  # type: ignore[arg-type]


def test_master_data_requires_all_entities():
    with pytest.raises(ValidationError, match="missing entities"):
        validate_master_data([{"entity": "province"}])


def test_master_data_accepts_all_entities():
    records = [
        {"entity": "province"},
        {"entity": "district"},
        {"entity": "ward"},
    ]
    validate_master_data(records)


def test_empty_master_data_raises():
    with pytest.raises(ValidationError, match="empty"):
        validate_master_data([])


def test_ghn_native_statuses_are_accepted():
    """The GHN API's own status vocabulary must pass validation."""
    for status in ("cancel", "pending", "processing", "delivering", "delivered", "fail", "return"):
        validate_order(dict(VALID_ORDER, status=status))


def test_normalised_statuses_are_also_accepted():
    for status in ("cancelled", "picked", "in_transit", "failed", "returned"):
        validate_order(dict(VALID_ORDER, status=status))


def test_genuinely_unknown_status_is_rejected():
    with pytest.raises(ValidationError, match="unknown status"):
        validate_order(dict(VALID_ORDER, status="teleported"))


def test_normalise_status_maps_ghn_values():
    from validator import normalise_status

    assert normalise_status("delivering") == "in_transit"
    assert normalise_status("fail") == "failed"
    assert normalise_status("cancel") == "cancelled"
