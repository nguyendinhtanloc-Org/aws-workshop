"""Transform raw GHN order records into the DynamoDB item shape."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

REQUIRED_FIELDS = ("order_code", "created_at", "status")

STATUS_TIMES = {
    "pending": "picked_at",
    "picked": "in_transit_at",
    "in_transit": "delivered_at",
}


def _parse_ts(value: Any) -> datetime | None:
    """Parse a timestamp string into an aware datetime, or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def transform_order(order: dict[str, Any]) -> dict[str, Any]:
    """Convert a raw order into the DynamoDB item shape.

    Raises ValueError if a required field is missing or unusable, so the
    caller can route the record to the DLQ instead of writing bad data.
    """
    missing = [f for f in REQUIRED_FIELDS if not order.get(f)]
    if missing:
        raise ValueError(f"order missing required fields: {', '.join(missing)}")

    created_at = _parse_ts(order["created_at"])
    if created_at is None:
        raise ValueError(f"order {order['order_code']!r} has unparseable created_at")

    delivered_at = _parse_ts(order.get("delivered_at"))
    lead_time_hours = None
    if delivered_at is not None:
        lead_time_hours = round((delivered_at - created_at).total_seconds() / 3600, 2)

    item: dict[str, Any] = {
        "pk": f"ORDER#{order['order_code']}",
        "sk": f"{created_at:%Y-%m-%d}",
        "order_code": order["order_code"],
        "order_id": order.get("order_id"),
        "from_province_id": order.get("from_province_id"),
        "from_district_id": order.get("from_district_id"),
        "to_province_id": order.get("to_province_id"),
        "to_district_id": order.get("to_district_id"),
        "weight": order.get("weight"),
        "status": order["status"],
        "shipping_fee": order.get("shipping_fee"),
        "cod_amount": order.get("cod_amount"),
        "created_at": created_at.isoformat(),
        "delivered_at": delivered_at.isoformat() if delivered_at else None,
        "lead_time_hours": lead_time_hours,
        "ingested_at": datetime.now(UTC).isoformat(),
    }
    return item
