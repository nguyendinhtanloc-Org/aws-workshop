"""Schema validation for records coming out of the GHN API."""
from __future__ import annotations

from typing import Any

GHN_STATUSES = frozenset(
    {
        "ready_to_pick",
        "picking",
        "money_collect_picking",
        "picked",
        "storing",
        "sorting",
        "transporting",
        "delivering",
        "money_collect_delivering",
        "delivered",
        "delivery_fail",
        "waiting_to_return",
        "return",
        "return_transporting",
        "return_sorting",
        "returning",
        "return_fail",
        "returned",
        "cancel",
        "exception",
        "lost",
        "damage",
        "scrap",
    }
)

VALID_STATUSES = frozenset(
    {
        "pending",
        "picked",
        "in_transit",
        "delivered",
        "failed",
        "returned",
        "cancelled",
    }
)

STATUS_NORMALISATION = {
    "ready_to_pick": "pending",
    "picking": "picked",
    "money_collect_picking": "picked",
    "picked": "picked",
    "storing": "in_transit",
    "sorting": "in_transit",
    "transporting": "in_transit",
    "delivering": "in_transit",
    "money_collect_delivering": "in_transit",
    "delivered": "delivered",
    "delivery_fail": "failed",
    "waiting_to_return": "returned",
    "return": "returned",
    "return_transporting": "returned",
    "return_sorting": "returned",
    "returning": "returned",
    "return_fail": "failed",
    "returned": "returned",
    "cancel": "cancelled",
    "exception": "failed",
    "lost": "failed",
    "damage": "failed",
    "scrap": "failed",
}


def normalise_status(status: str) -> str:
    """Map a GHN status onto the project's normalised status."""
    try:
        return STATUS_NORMALISATION[status]
    except KeyError:
        raise ValueError(f"unknown GHN status {status!r}") from None


REQUIRED_ORDER_FIELDS = (
    "order_code",
    "status",
    "created_at",
)


class ValidationError(ValueError):
    """Raised when a record does not satisfy the expected schema."""


def validate_order(order: dict[str, Any]) -> None:
    """Validate a single order record."""
    if not isinstance(order, dict):
        raise ValidationError(f"order must be an object, got {type(order).__name__}")

    missing = [f for f in REQUIRED_ORDER_FIELDS if not order.get(f)]
    if missing:
        raise ValidationError(f"order missing required fields: {', '.join(missing)}")

    status = order["status"]
    if status not in GHN_STATUSES and status not in VALID_STATUSES:
        raise ValidationError(
            f"order {order.get('order_code')!r} has unknown status {status!r}"
        )

    weight = order.get("weight")
    if weight is not None and (not isinstance(weight, int | float) or weight <= 0):
        raise ValidationError(f"order {order.get('order_code')!r} has invalid weight {weight!r}")


def validate_master_data(records: list[dict[str, Any]]) -> None:
    """Validate the master data record list as a whole."""
    if not records:
        raise ValidationError("master data is empty")

    entities = {r.get("entity") for r in records}
    missing = {"province", "district"} - entities
    if missing:
        raise ValidationError(f"master data missing entities: {', '.join(sorted(missing))}")

    for record in records:
        if not record.get("entity"):
            raise ValidationError(f"master record missing 'entity': {record!r}")
