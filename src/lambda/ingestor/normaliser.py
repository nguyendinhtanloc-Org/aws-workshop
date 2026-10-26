"""Map GHN API responses onto the internal order record shape.

The raw store keeps GHN payloads verbatim (PascalCase, GHN field names).
The Ingestor applies this mapping at the boundary before validation, so
downstream stages (Processor, Glue) only ever see the internal schema.

Source shape (confirmed from a live dev-gateway order detail call):

    data.order_code, data.client_order_code, data.status,
    data.created_date / data.updated_date (ISO 8601), data.weight (g),
    data.cod_amount / data.insurance_value / data.order_value (VND),
    data.length/width/height (cm), data.service_type_id, ...

Notes:
- GHN detail responses carry no province/district ids - addresses are
  free-text. The warehouse treats those columns as optional joins, not
  required fields.
- created_at (internal) comes from GHN's created_date.
"""
from __future__ import annotations

from typing import Any

# GHN detail field -> internal field, when names differ.
FIELD_MAP = {
    "created_date": "created_at",
    "updated_date": "updated_at",
}

# Fields copied straight through (same name, same semantics/units).
PASSTHROUGH_FIELDS = (
    "order_code",
    "client_order_code",
    "status",
    "weight",        # grams
    "cod_amount",    # VND
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
    """Turn one GHN shipping-order detail object into an internal record.

    GHN-native field names are renamed (created_date -> created_at) and
    kept; fields that already use the internal names pass through
    unchanged, so records produced by the offline mock need no separate
    handling. Raises ValueError when order_code / status / created_at
    are absent after renaming - the Ingestor routes those to the DLQ.
    """
    if not isinstance(detail, dict):
        raise ValueError(f"order detail must be an object, got {type(detail).__name__}")

    record = dict(detail)

    for src, dst in FIELD_MAP.items():
        if src in record:
            record.setdefault(dst, record.pop(src))

    # The detail response carries no numeric order id; the tracking
    # number is the stable unique key the pipeline uses end to end.
    record.setdefault("order_id", record.get("order_code"))

    missing = [f for f in ("order_code", "status", "created_at") if not record.get(f)]
    if missing:
        raise ValueError(
            f"GHN order detail for {record.get('order_code')!r} missing {', '.join(missing)}"
        )
    return record
