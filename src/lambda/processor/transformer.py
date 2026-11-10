"""Transform raw GHN order records into the DynamoDB item shape."""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

REQUIRED_FIELDS = ("order_code", "created_at", "status")

# Maps the GHN API's status vocabulary onto the normalised one the
# warehouse and the dashboards use. Kept here rather than imported from
# the Ingestor package, because the two packages are deployed as separate
# Lambda zips and cannot share modules. The source vocabulary is from the
# GHN docs' "Order Status Codes" page.
STATUS_NORMALISATION = {
    # pickup phase
    "ready_to_pick": "pending",
    "picking": "picked",
    "money_collect_picking": "picked",
    "picked": "picked",
    # warehouse / transit phase
    "storing": "in_transit",
    "sorting": "in_transit",
    "transporting": "in_transit",
    "delivering": "in_transit",
    "money_collect_delivering": "in_transit",
    # delivered
    "delivered": "delivered",
    # delivery failure / return-to-sender
    "delivery_fail": "failed",
    "waiting_to_return": "returned",
    "return": "returned",
    "return_transporting": "returned",
    "return_sorting": "returned",
    "returning": "returned",
    "return_fail": "failed",
    "returned": "returned",
    # cancelled / abnormal finals
    "cancel": "cancelled",
    "exception": "failed",
    "lost": "failed",
    "damage": "failed",
    "scrap": "failed",
}

# The 7 terminal states in the GHN lifecycle (including both raw "cancel" and normalised "cancelled").
# Once an order reaches one of these states, no subsequent non-terminal or stale event may overwrite it.
TERMINAL_STATUSES = frozenset(
    {
        "delivered",
        "returned",
        "cancel",
        "cancelled",
        "exception",
        "lost",
        "damage",
        "scrap",
    }
)

# Local phase ordering within a single delivery_attempt.
# Protects against clock skew between sequential states in the same attempt
# while still allowing a new delivery_attempt (retry loop) to reset phase to 40 (delivering).
PHASE_RANK = {
    "ready_to_pick": 10,
    "pending": 10,
    "picking": 20,
    "money_collect_picking": 20,
    "picked": 20,
    "storing": 30,
    "sorting": 30,
    "transporting": 30,
    "in_transit": 30,
    "delivering": 40,
    "money_collect_delivering": 40,
    "delivery_fail": 50,
    "failed": 50,
    "waiting_to_return": 60,
    "return": 60,
    "return_transporting": 60,
    "return_sorting": 60,
    "returning": 60,
    "return_fail": 65,
    "delivered": 100,
    "returned": 100,
    "cancel": 100,
    "cancelled": 100,
    "exception": 100,
    "lost": 100,
    "damage": 100,
    "scrap": 100,
}

# Numeric fields that must be written as Decimal.
#
# boto3's DynamoDB type serializer raises TypeError on Python floats:
# "Float types are not supported. Use Decimal types instead." The GHN API
# returns JSON numbers, which json.loads parses as float, so every order
# with a fractional weight or a fractional lead time would fail the
# put_item call and never land in the table. Every numeric field is
# converted to Decimal here, at the single point where the item shape
# is defined.
NUMERIC_FIELDS = (
    "weight",
    "shipping_fee",
    "cod_amount",
    "lead_time_hours",
    "from_province_id",
    "from_district_id",
    "to_province_id",
    "to_district_id",
)


def _to_decimal(value: Any) -> Any:
    """Convert a number to Decimal, leaving everything else untouched.

    Ints stay ints (DynamoDB serialises them fine) but they are converted
    too so that a value arriving as a float-typed int stays exact.
    """
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, int):
        return Decimal(value)
    return value


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


def _compute_delivery_attempt(order: dict[str, Any], updated_at: datetime) -> int:
    """Compute delivery_attempt = 1 + number of prior delivery_fail transitions from log."""
    if order.get("delivery_attempt") is not None:
        return int(order["delivery_attempt"])
    log = order.get("log")
    if not isinstance(log, list) or not log:
        return 1
    raw_status = str(order.get("status", ""))
    last_entry = log[-1] if isinstance(log[-1], dict) else {}
    last_ts = _parse_ts(last_entry.get("updated_date"))
    if last_entry.get("status") == raw_status and (last_ts is None or last_ts == updated_at):
        prior_entries = log[:-1]
    else:
        prior_entries = [
            e
            for e in log
            if isinstance(e, dict)
            and (_parse_ts(e.get("updated_date")) is None or _parse_ts(e.get("updated_date")) < updated_at)
        ]
    prior_fails = sum(
        1 for e in prior_entries if isinstance(e, dict) and e.get("status") in ("delivery_fail", "failed")
    )
    return 1 + prior_fails


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
    updated_at = _parse_ts(order.get("updated_at")) or delivered_at or created_at
    lead_time_hours = None
    if delivered_at is not None:
        lead_time_hours = round((delivered_at - created_at).total_seconds() / 3600, 2)

    raw_status = str(order["status"])
    norm_status = STATUS_NORMALISATION.get(raw_status, raw_status)
    if order.get("is_terminal") is not None:
        is_terminal = bool(order["is_terminal"])
    else:
        is_terminal = raw_status in TERMINAL_STATUSES
    delivery_attempt = _compute_delivery_attempt(order, updated_at)
    phase_rank = int(order["phase_rank"]) if order.get("phase_rank") is not None else PHASE_RANK.get(raw_status, 0)

    item: dict[str, Any] = {
        "pk": f"ORDER#{order['order_code']}",
        "sk": f"{created_at:%Y-%m-%d}",
        "order_code": order["order_code"],
        "order_id": order.get("order_id"),
        "status": norm_status,
        "is_terminal": is_terminal,
        "delivery_attempt": delivery_attempt,
        "phase_rank": phase_rank,
        "created_at": created_at.isoformat(),
        "delivered_at": delivered_at.isoformat() if delivered_at else None,
        "updated_at": updated_at.isoformat(),
        "ingested_at": datetime.now(UTC).isoformat(),
    }

    for field in NUMERIC_FIELDS:
        item[field] = _to_decimal(order.get(field))

    item["lead_time_hours"] = _to_decimal(lead_time_hours)

    return item
