"""Shared pytest fixtures for the GHN pipeline test suite."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_order() -> dict[str, Any]:
    """A minimal, valid order record."""
    return {
        "order_id": "9001",
        "order_code": "GHN-TEST-001",
        "from_province_id": 20,
        "from_district_id": 268,
        "to_province_id": 79,
        "to_district_id": 760,
        "weight": 2.5,
        "status": "delivered",
        "created_at": "2026-01-01T08:00:00Z",
        "delivered_at": "2026-01-01T14:30:00Z",
        "shipping_fee": 32000,
        "cod_amount": 150000,
    }


@pytest.fixture
def sample_order_batch(sample_order: dict[str, Any]) -> list[dict[str, Any]]:
    """A batch of five orders with distinct codes."""
    return [{**sample_order, "order_code": f"GHN-TEST-{i:03d}"} for i in range(5)]


@pytest.fixture
def write_json(tmp_path: Path):
    """Write a payload to a temp JSON file and return its path."""

    def _write(name: str, payload: Any) -> str:
        path = tmp_path / name
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return str(path)

    return _write
