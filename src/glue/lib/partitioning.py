"""Partition layout for the GHN curated and silver layers."""
from __future__ import annotations

from datetime import UTC, datetime


def raw_key(source: str, timestamp: datetime | None = None) -> str:
    """Key for a raw landing object, partitioned by source and ingest date."""
    ts = timestamp or datetime.now(UTC)
    return f"raw/{source}/{ts:%Y-%m-%d}/{ts:%Y%m%dT%H%M%S%f}.json"


def curated_prefix(source: str) -> str:
    """Prefix for the curated (Firehose-delivered) layer."""
    return f"curated/{source}/"


def silver_prefix() -> str:
    """Prefix for the silver (typed, cleaned) layer."""
    return "silver/orders/"


def gold_prefix() -> str:
    """Prefix for the gold (aggregate) layer."""
    return "gold/"
