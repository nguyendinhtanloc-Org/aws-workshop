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
    """Writes order items with a conditional put so replays are no-ops."""

    def __init__(self, table: Any) -> None:
        self._table = table

    def put(self, item: dict[str, Any]) -> WriteResult:
        """Write one item. Returns DUPLICATE if the key already exists."""
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
            )
        except Exception as exc:  # botocore ClientError
            if "ConditionalCheckFailed" in str(exc):
                logger.info("duplicate skipped pk=%s sk=%s", item.get("pk"), item.get("sk"))
                return WriteResult.DUPLICATE
            raise
        return WriteResult.WRITTEN
