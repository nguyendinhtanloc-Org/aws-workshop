"""Tests for the data quality rules."""
from __future__ import annotations

from lib.quality import (
    check_duplicate_codes,
    check_lead_time_range,
    check_required_fields,
    check_valid_status,
    run_all,
)

RECORDS = [
    {"order_code": "A1", "created_at": "2026-01-01", "status": "delivered", "lead_time_hours": 5},
    {"order_code": "A2", "created_at": "2026-01-01", "status": "failed", "lead_time_hours": 900},
]


def test_required_fields_detects_missing():
    violations = check_required_fields([{"order_code": "A1"}], ("order_code", "status"))
    assert violations == ["A1: missing status"]


def test_valid_status_detects_unknown():
    violations = check_valid_status([{"order_code": "A1", "status": "teleported"}])
    assert len(violations) == 1
    assert "unknown status" in violations[0]


def test_lead_time_range_flags_outliers():
    violations = check_lead_time_range(RECORDS)
    assert len(violations) == 1
    assert "900" in violations[0]


def test_duplicate_codes_detected():
    violations = check_duplicate_codes([{"order_code": "A1"}, {"order_code": "A1"}])
    assert violations == ["duplicate order_code A1"]


def test_run_all_groups_by_rule():
    result = run_all(RECORDS)
    assert set(result) == {
        "required_fields",
        "valid_status",
        "lead_time_range",
        "duplicate_codes",
    }
    assert result["lead_time_range"]
    assert not result["duplicate_codes"]
