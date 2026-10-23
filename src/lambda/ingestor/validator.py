"""Schema validation for records coming out of the GHN API.

The validator is deliberately strict: a record that cannot be validated is
rejected at the boundary rather than being silently written downstream.
"""
from __future__ import annotations

from typing import Any

VALID_STATUSES = frozenset(
    {"pending", "picked", "in_transit", "delivered", "failed", "returned"}
)

REQUIRED_ORDER_FIELDS = (
    "order_id",
    "order_code",
    "from_province_id",
    "to_province_id",
    "status",
    "created_at",
)


class ValidationError(ValueError):
    """Raised when a record does not satisfy the expected schema."""


def validate_order(order: dict[str, Any]) -> None:
    """Validate a single order record. Raises ValidationError on failure."""
    if not isinstance(order, dict):
        raise ValidationError(f"order must be an object, got {type(order).__name__}")

    missing = [f for f in REQUIRED_ORDER_FIELDS if not order.get(f)]
    if missing:
        raise ValidationError(f"order missing required fields: {', '.join(missing)}")

    status = order["status"]
    if status not in VALID_STATUSES:
        raise ValidationError(
            f"order {order.get('order_code')!r} has unknown status {status!r}"
        )

    weight = order.get("weight")
    if weight is not None and (not isinstance(weight, (int, float)) or weight <= 0):
        raise ValidationError(f"order {order.get('order_code')!r} has invalid weight {weight!r}")


def validate_master_data(records: list[dict[str, Any]]) -> None:
    """Validate the master data record list as a whole."""
    if not records:
        raise ValidationError("master data is empty")

    entities = {r.get("entity") for r in records}
    missing = {"province", "district", "ward"} - entities
    if missing:
        raise ValidationError(f"master data missing entities: {', '.join(sorted(missing))}")

    for record in records:
        if not record.get("entity"):
            raise ValidationError(f"master record missing 'entity': {record!r}")
