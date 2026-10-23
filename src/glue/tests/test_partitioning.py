"""Tests for the partition key layout."""
from __future__ import annotations

from datetime import UTC, datetime

from lib.partitioning import curated_prefix, gold_prefix, raw_key, silver_prefix


def test_raw_key_is_source_and_date_partitioned():
    key = raw_key("orders", datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
    assert key.startswith("raw/orders/2026-01-02/")
    assert key.endswith(".json")


def test_layer_prefixes():
    assert curated_prefix("orders") == "curated/orders/"
    assert silver_prefix() == "silver/orders/"
    assert gold_prefix() == "gold/"
