"""Map GHN API responses onto the internal order record shape."""
from __future__ import annotations

from typing import Any

FIELD_MAP = {
    "created_date": "created_at",
    "updated_date": "updated_at",
    "finish_date": "delivered_at",
}

PASSTHROUGH_FIELDS = (
    "order_code",
    "client_order_code",
    "status",
    "weight",
    "cod_amount",
    "cod_failed_amount",
    "insurance_value",
    "order_value",
    "length",
    "width",
    "height",
    "service_type_id",
    "payment_type_id",
    "to_name",
    "to_phone",
    "to_address",
    "from_name",
    "from_address",
)


def normalise_order(detail: dict[str, Any]) -> dict[str, Any]:
    """Turn one GHN shipping-order detail object into an internal record."""
    if not isinstance(detail, dict):
        raise ValueError(f"order detail must be an object, got {type(detail).__name__}")

    record = dict(detail)

    for src, dst in FIELD_MAP.items():
        if src in record:
            record.setdefault(dst, record.pop(src))

    record.setdefault("order_id", record.get("order_code"))

    missing = [f for f in ("order_code", "status", "created_at") if not record.get(f)]
    if missing:
        raise ValueError(
            f"GHN order detail for {record.get('order_code')!r} missing {', '.join(missing)}"
        )
    return record
