"""A deterministic fake GHN API, for local runs and for the workshop demo.

The real GHN API is not always reachable from a laptop - the hosts have
changed between API generations and corporate networks and VPNs often
block them. This module produces records in exactly the shape the real
API returns, so the whole pipeline (ingest -> S3 -> Kinesis -> Lambda ->
DynamoDB -> Glue -> Athena) can be exercised without it.

The generated data is deliberately imperfect: some records are missing
required fields, some carry an unknown status. That is what exercises
the quarantine path, the Decimal conversion and the status
normalisation in the normal way. Weights are integers in grams, per
GHN's API conventions.
"""
from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from typing import Any

PROVINCES = [
    {"province_id": 79, "province_name": "Hồ Chí Minh"},
    {"province_id": 20, "province_name": "Hà Nội"},
    {"province_id": 31, "province_name": "Hải Phòng"},
    {"province_id": 44, "province_name": "Đà Nẵng"},
]

# GHN's real status values, from the docs' "Order Status Codes" page.
STATUSES = [
    "ready_to_pick",
    "picking",
    "money_collect_picking",
    "picked",
    "storing",
    "sorting",
    "transporting",
    "delivering",
    "money_collect_delivering",
    "delivered",
    "delivery_fail",
    "waiting_to_return",
    "return",
    "return_transporting",
    "return_sorting",
    "returning",
    "return_fail",
    "returned",
    "cancel",
    "exception",
    "lost",
    "damage",
    "scrap",
]


class MockGhnClient:
    """Stands in for GhnClient, returning generated data.

    The interface matches GhnClient exactly: fetch_master_data,
    fetch_orders and fetch_shipping_status.
    """

    def __init__(
        self,
        token: str = "",
        base_url: str = "",
        shop_id: str = "",
        seed: int | None = 7,
    ) -> None:
        self._token = token
        self._base_url = base_url
        self._shop_id = shop_id
        self._rng = random.Random(seed)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Mimic the client's GET interface for the probe script.

        Matches on the last path segment so it works for both the
        legacy apiv3 names (getProvinces) and the current generation
        (provinces).
        """
        segment = path.rstrip("/").rsplit("/", 1)[-1].lower()
        if segment in ("provinces", "getprovinces"):
            return {"code": 200, "data": PROVINCES}
        if segment in ("districts", "getdistricts"):
            return {"code": 200, "data": [{"district_id": i, "district_name": f"District {i}"} for i in range(1, 6)]}
        if segment in ("wards", "getward"):
            return {"code": 200, "data": [{"ward_code": f"w{i}", "ward_name": f"Ward {i}"} for i in range(1, 6)]}
        if segment in ("orders", "getorder", "getorderlist"):
            return {"code": 200, "data": self.fetch_orders()}
        raise AssertionError(f"unmocked path {path}")

    def fetch_master_data(self) -> list[dict[str, Any]]:
        """Provinces, districts and wards as one flat record list."""
        records: list[dict[str, Any]] = []
        for entity, items in (
            ("province", PROVINCES),
            ("district", [{"district_id": i, "district_name": f"District {i}"} for i in range(1, 6)]),
            ("ward", [{"ward_code": f"w{i}", "ward_name": f"Ward {i}"} for i in range(1, 6)]),
        ):
            for item in items:
                records.append({"entity": entity, **item})
        return records

    def fetch_orders(self, count: int = 12) -> list[dict[str, Any]]:
        """Generate a batch of orders, some of them deliberately broken."""
        now = datetime.now(UTC)
        orders: list[dict[str, Any]] = []
        for i in range(count):
            created = now - timedelta(hours=self._rng.randint(1, 72))
            status = self._rng.choice(STATUSES)
            order: dict[str, Any] = {
                "order_id": str(900_000 + i),
                "order_code": f"GHNMOCK{i:04d}",
                "from_province_id": self._rng.choice(PROVINCES)["province_id"],
                "from_district_id": self._rng.randint(1, 5),
                "to_province_id": self._rng.choice(PROVINCES)["province_id"],
                "to_district_id": self._rng.randint(1, 5),
                # GHN's convention is grams, not kilograms.
                "weight": self._rng.randint(500, 20_000),
                "status": status,
                "created_at": created.isoformat(),
                "shipping_fee": self._rng.randint(20_000, 200_000),
                "cod_amount": self._rng.randint(0, 3_000_000),
            }
            if status == "delivered":
                order["delivered_at"] = (
                    created + timedelta(hours=self._rng.randint(6, 72))
                ).isoformat()
            else:
                # Not yet delivered: no delivered_at.
                order["delivered_at"] = None

            # Deliberate imperfections, so the pipeline's error handling is
            # actually exercised rather than assumed.
            if i % 7 == 3:
                order.pop("to_province_id")          # missing required field
            elif i % 7 == 5:
                order["status"] = "teleported"        # unknown status

            orders.append(order)
        return orders

    def fetch_shipping_status(self, count: int = 8) -> list[dict[str, Any]]:
        """Generate shipping status history records."""
        now = datetime.now(UTC)
        return [
            {
                "order_code": f"GHNMOCK{i:04d}",
                "status": self._rng.choice(STATUSES),
                "status_time": (now - timedelta(hours=i * 3)).isoformat(),
                "note": self._rng.choice(["", "hub scan", "delivery attempt 1"]),
            }
            for i in range(count)
        ]
