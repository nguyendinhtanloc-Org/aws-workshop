"""Idempotent writes to the DynamoDB orders table."""
from __future__ import annotations

import logging
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class WriteResult(Enum):
    WRITTEN = "written"
    DUPLICATE = "duplicate"


class OrderWriter:
    """Writes order items with a conditional put supporting out-of-order protection and idempotent replays."""

    def __init__(self, table: Any) -> None:
        self._table = table

    def put(self, item: dict[str, Any]) -> WriteResult:
        """Write or update one item according to the Two-Level State Machine.

        Conditions:
        - If the item is new (pk and sk do not exist), it is written.
        - If the existing item is NOT terminal (or is_terminal is absent):
          Allow update if:
            - incoming is_terminal is True
            - OR incoming delivery_attempt > existing.delivery_attempt
            - OR (incoming delivery_attempt == existing.delivery_attempt AND incoming updated_at > existing.updated_at)
        - If existing item is terminal:
          NO non-terminal state or older state can overwrite it.
        """
        updated_at = item.get("updated_at")
        is_terminal = bool(item.get("is_terminal", False))
        delivery_attempt = int(item.get("delivery_attempt", 1))
        phase_rank = int(item.get("phase_rank", 0))

        try:
            if updated_at:
                self._table.put_item(
                    Item=item,
                    ConditionExpression=(
                        "(attribute_not_exists(pk) AND attribute_not_exists(sk)) "
                        "OR ("
                        "(attribute_not_exists(is_terminal) OR is_terminal = :false) "
                        "AND ("
                        ":is_terminal = :true "
                        "OR attribute_not_exists(delivery_attempt) "
                        "OR delivery_attempt < :new_attempt "
                        "OR (delivery_attempt = :new_attempt AND ("
                        "attribute_not_exists(phase_rank) "
                        "OR phase_rank < :new_phase_rank "
                        "OR (phase_rank = :new_phase_rank AND (attribute_not_exists(updated_at) OR updated_at < :new_updated_at))"
                        "))"
                        ")"
                        ")"
                    ),
                    ExpressionAttributeValues={
                        ":new_updated_at": updated_at,
                        ":is_terminal": is_terminal,
                        ":new_attempt": delivery_attempt,
                        ":new_phase_rank": phase_rank,
                        ":false": False,
                        ":true": True,
                    },
                )
            else:
                self._table.put_item(
                    Item=item,
                    ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
                )
        except Exception as exc:  # botocore ClientError
            if "ConditionalCheckFailed" in str(exc):
                logger.info(
                    "stale, terminal or duplicate skipped pk=%s sk=%s updated_at=%s attempt=%s",
                    item.get("pk"),
                    item.get("sk"),
                    updated_at,
                    delivery_attempt,
                )
                return WriteResult.DUPLICATE
            raise
        return WriteResult.WRITTEN
