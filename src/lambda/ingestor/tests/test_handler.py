"""Tests for the Ingestor handler's batch-level behaviour.

The handler's job is to make sure a single bad record cannot cost us the
whole batch. These tests exercise that partitioning directly.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from handler import _partition_valid  # noqa: E402

GOOD = {
    "order_id": "1",
    "order_code": "GHN1",
    "from_province_id": 1,
    "to_province_id": 2,
    "status": "pending",
    "created_at": "2026-01-01T00:00:00Z",
}

BAD_STATUS = dict(GOOD, order_code="GHN2", status="teleported")
MISSING_FIELD = {k: v for k, v in dict(GOOD, order_code="GHN3").items() if k != "order_code"}


def test_valid_records_pass_through():
    good, rejected = _partition_valid("orders", [dict(GOOD)])
    assert len(good) == 1
    assert rejected == []


def test_one_bad_record_does_not_discard_the_batch():
    good, rejected = _partition_valid("orders", [dict(GOOD), dict(BAD_STATUS), dict(GOOD, order_code="GHN4")])
    assert len(good) == 2
    assert len(rejected) == 1


def test_rejected_record_keeps_the_original_and_the_reason():
    _, rejected = _partition_valid("orders", [dict(BAD_STATUS)])
    assert rejected[0]["record"]["order_code"] == "GHN2"
    assert "unknown status" in rejected[0]["reason"]


def test_missing_field_is_rejected():
    good, rejected = _partition_valid("orders", [dict(MISSING_FIELD)])
    assert good == []
    assert len(rejected) == 1


def test_non_order_sources_are_not_record_validated():
    """Master data and shipping status are validated as a whole, not per record."""
    records = [{"entity": "province"}, {"anything": 1}]
    good, rejected = _partition_valid("master_data", records)
    assert good == records
    assert rejected == []
