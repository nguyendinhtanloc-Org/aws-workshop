"""A deterministic fake GHN API, for local runs and for the workshop demo."""
from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from typing import Any

PROVINCES = [
    {"ProvinceID": 79, "ProvinceName": "Hồ Chí Minh", "Code": "44", "CountryID": 1},
    {"ProvinceID": 20, "ProvinceName": "Hà Nội", "Code": "4", "CountryID": 1},
    {"ProvinceID": 31, "ProvinceName": "Hải Phòng", "Code": "45", "CountryID": 1},
    {"ProvinceID": 44, "ProvinceName": "Đà Nẵng", "Code": "29", "CountryID": 1},
]

DISTRICTS = [
    {"DistrictID": i, "ProvinceID": p, "DistrictName": n, "DistrictEncode": f"D{i}",
     "Status": 1, "PickType": 1}
    for i, p, n in (
        (2, 20, "Ba Đình"), (3, 20, "Hoàn Kiếm"),
        (5, 79, "Quận 1"), (6, 79, "Quận 3"),
        (8, 44, "Hải Châu"), (9, 44, "Thanh Khê"),
    )
]

WARDS = [
    {"WardCode": f"w{i}", "DistrictID": i, "WardName": f"Ward {i}"}
    for i in (2, 3, 5, 6, 8, 9)
]

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
    """Stands in for GhnClient, returning generated data."""

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
        """Mimic the client's GET interface for the probe script."""
        segment = path.rstrip("/").rsplit("/", 1)[-1].lower()
        if segment in ("province", "provinces", "getprovinces"):
            return {"code": 200, "data": PROVINCES}
        if segment in ("district", "districts", "getdistricts"):
            return {"code": 200, "data": DISTRICTS}
        if segment in ("ward", "wards", "getward"):
            return {"code": 200, "data": WARDS}
        if segment in ("detail", "orders", "getorder", "getorderlist"):
            return {"code": 200, "data": self.fetch_orders()}
        raise AssertionError(f"unmocked path {path}")

    def fetch_master_data(
        self, ward_district_ids: list[int] | None = None
    ) -> list[dict[str, Any]]:
        """Return mock provinces, districts and wards for the requested districts."""
        records: list[dict[str, Any]] = []
        for entity, items in (("province", PROVINCES), ("district", DISTRICTS)):
            for item in items:
                records.append({"entity": entity, **item})
        for district_id in ward_district_ids or []:
            for ward in WARDS:
                if ward.get("DistrictID") == district_id:
                    records.append({"entity": "ward", **ward})
        return records

    def fetch_orders(
        self,
        order_codes: list[str] | None = None,
        count: int = 12,
    ) -> list[dict[str, Any]]:
        """Generate a batch of orders, some of them deliberately broken."""
        if order_codes:
            return [
                self._make_detail(i, code, broken=(i % 7 in (3, 5)))
                for i, code in enumerate(order_codes)
            ]
        codes = [f"GHNMOCK{i:04d}" for i in range(count)]
        return [self._make_order(i, code) for i, code in enumerate(codes)]

    def _make_detail(
        self, index: int, code: str, broken: bool = False
    ) -> dict[str, Any]:
        """A GHN shipping-order detail record (detail-route field names)."""
        created = datetime.now(UTC) - timedelta(hours=self._rng.randint(1, 72))
        record: dict[str, Any] = {
            "order_code": code,
            "client_order_code": f"SHOP-{code}",
            "status": self._rng.choice(STATUSES),
            "created_date": created.isoformat(),
            "updated_date": created.isoformat(),
            "weight": self._rng.randint(500, 20_000),
            "cod_amount": self._rng.randint(0, 3_000_000),
            "insurance_value": 0,
            "order_value": 0,
            "length": 25,
            "width": 20,
            "height": 8,
            "service_type_id": 2,
            "payment_type_id": 2,
            "to_name": "Người Nhận Test",
            "to_phone": "0900000000",
            "to_address": "Địa chỉ test",
        }
        if record["status"] == "delivered":
            record["delivered_date"] = (
                created + timedelta(hours=self._rng.randint(6, 72))
            ).isoformat()
        if broken:
            if index % 2:
                record["status"] = "teleported"
            else:
                record.pop("order_code")
        return record

    def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Mimic the create-order route: assign a code, echo the fee block."""
        code = f"LADMO{self._rng.randint(100, 999)}"
        return {
            "order_code": code,
            "fee": {"main_service": self._rng.randint(10_000, 40_000), "total": 0},
            "total_fee": self._rng.randint(10_000, 40_000),
            "expected_delivery_time": datetime.now(UTC).isoformat(),
        }

    def _make_order(self, index: int, code: str) -> dict[str, Any]:
        """Build one mock order record, optionally broken on purpose."""
        created = datetime.now(UTC) - timedelta(hours=self._rng.randint(1, 72))
        status = self._rng.choice(STATUSES)
        order: dict[str, Any] = {
            "order_id": str(900_000 + index),
            "order_code": code,
            "from_province_id": self._rng.choice(PROVINCES)["ProvinceID"],
            "from_district_id": self._rng.randint(1, 9),
            "to_province_id": self._rng.choice(PROVINCES)["ProvinceID"],
            "to_district_id": self._rng.randint(1, 9),
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
            order["delivered_at"] = None

        if index % 7 == 3:
            order.pop("to_province_id")
        elif index % 7 == 5:
            order["status"] = "teleported"
        return order

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
