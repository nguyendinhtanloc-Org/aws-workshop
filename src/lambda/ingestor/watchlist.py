"""Shared watchlist registration; resource API preserves Python value types."""

import os
from datetime import UTC, datetime


def is_terminal(status: str) -> bool:
    """Return whether this GHN status ends watchlist tracking."""
    return status in {
        "delivered", "returned", "cancel", "cancelled", "exception", "lost", "damage", "scrap"
    }


def register_order(code: str) -> None:
    name = os.environ.get("ORDER_WATCHLIST_TABLE")
    if not name:
        return
    import boto3

    boto3.resource("dynamodb").Table(name).update_item(
        Key={"pk": "#WATCH", "sk": code},
        UpdateExpression="SET registered_at = if_not_exists(registered_at, :now), terminal = if_not_exists(terminal, :false)",
        ExpressionAttributeValues={":now": datetime.now(UTC).isoformat(), ":false": False},
    )
