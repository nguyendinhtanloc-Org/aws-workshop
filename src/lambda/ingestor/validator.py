"""Schema validation for records coming out of the GHN API.

The validator is deliberately strict: a record that cannot be validated is
rejected at the boundary rather than being silently written downstream.
"""
from __future__ import annotations

from typing import Any

# GHN's status vocabulary, from the GHN docs' "Order Status Codes" page.
# GHN returns the raw values; the transform layer maps them onto the
# normalised set.
GHN_STATUSES = frozenset(
    {
        # pickup phase
        "ready_to_pick",
        "picking",
        "money_collect_picking",
        "picked",
        # warehouse / transit phase
        "storing",
        "sorting",
        "transporting",
        "delivering",
        "money_collect_delivering",
        # final: delivered
        "delivered",
        # delivery-failure / return-to-sender phase
        "delivery_fail",
        "waiting_to_return",
        "return",
        "return_transporting",
        "return_sorting",
        "returning",
        "return_fail",
        # final: returned
        "returned",
        # final: cancelled / abnormal
        "cancel",
        "exception",
        "lost",
        "damage",
        "scrap",
    }
)

# The normalised vocabulary used by the warehouse and the dashboards.
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

# Maps each GHN status onto the normalised one. Anything not listed
# here is rejected rather than silently bucketed.
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
    """Map a GHN status onto the project's normalised status.

    Raises ValueError for an unknown status.
    """
    try:
        return STATUS_NORMALISATION[status]
    except KeyError:
        raise ValueError(f"unknown GHN status {status!r}") from None


# The GHN order detail guarantees order_code, status and a creation
# timestamp; province/district ids are NOT in the API response (addresses
# are free text), so they stay optional throughout the warehouse.
# order_id is optional too: the normaliser falls back to order_code.
REQUIRED_ORDER_FIELDS = (
    "order_code",
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
    if status not in GHN_STATUSES and status not in VALID_STATUSES:
        raise ValidationError(
            f"order {order.get('order_code')!r} has unknown status {status!r}"
        )

    weight = order.get("weight")
    if weight is not None and (not isinstance(weight, int | float) or weight <= 0):
        raise ValidationError(f"order {order.get('order_code')!r} has invalid weight {weight!r}")


def validate_master_data(records: list[dict[str, Any]]) -> None:
    """Validate the master data record list as a whole.

    Province and district are required; ward is optional, because the
    ward endpoint only answers per district and the Ingestor fetches
    wards for a bounded district list.
    """
    if not records:
        raise ValidationError("master data is empty")

    entities = {r.get("entity") for r in records}
    missing = {"province", "district"} - entities
    if missing:
        raise ValidationError(f"master data missing entities: {', '.join(sorted(missing))}")

    for record in records:
        if not record.get("entity"):
            raise ValidationError(f"master record missing 'entity': {record!r}")
