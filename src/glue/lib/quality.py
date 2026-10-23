"""Data quality rules applied to the GHN pipeline.

Each rule returns a list of violations. A non-empty list blocks promotion
to the next layer. Keeping them as plain functions (not a framework) means
they can be unit tested without a Glue session.
"""
from __future__ import annotations

from typing import Any

VALID_STATUSES = frozenset(
    {"pending", "picked", "in_transit", "delivered", "failed", "returned"}
)


def check_required_fields(records: list[dict[str, Any]], fields: tuple[str, ...]) -> list[str]:
    """Every record must have a value for every required field."""
    violations: list[str] = []
    for record in records:
        for field in fields:
            if record.get(field) in (None, ""):
                violations.append(f"{record.get('order_code', '?')}: missing {field}")
    return violations


def check_valid_status(records: list[dict[str, Any]]) -> list[str]:
    """Every status must be in the known set."""
    return [
        f"{record.get('order_code', '?')}: unknown status {record.get('status')!r}"
        for record in records
        if record.get("status") not in VALID_STATUSES
    ]


def check_lead_time_range(records: list[dict[str, Any]], max_hours: float = 720.0) -> list[str]:
    """Lead time must be non-negative and below the plausible ceiling."""
    violations: list[str] = []
    for record in records:
        lead = record.get("lead_time_hours")
        if lead is None:
            continue
        if lead < 0 or lead > max_hours:
            violations.append(f"{record.get('order_code', '?')}: lead_time_hours={lead}")
    return violations


def check_duplicate_codes(records: list[dict[str, Any]]) -> list[str]:
    """Order codes must be unique within a batch."""
    seen: set[str] = set()
    violations: list[str] = []
    for record in records:
        code = record.get("order_code")
        if code and code in seen:
            violations.append(f"duplicate order_code {code}")
        if code:
            seen.add(code)
    return violations


def run_all(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Run every quality rule and return violations grouped by rule."""
    return {
        "required_fields": check_required_fields(
            records, ("order_code", "created_at", "status")
        ),
        "valid_status": check_valid_status(records),
        "lead_time_range": check_lead_time_range(records),
        "duplicate_codes": check_duplicate_codes(records),
    }
