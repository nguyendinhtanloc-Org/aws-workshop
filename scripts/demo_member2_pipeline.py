#!/usr/bin/env python3
"""Demo trực quan end-to-end cho Member 2:
1. Nhận chuỗi sự kiện SQS FIFO của một đơn hàng GHN.
2. Xử lý qua Transformer + Two-Level State Machine.
3. Ghi vào DynamoDB và in ra cấu trúc bảng cho CSKH.
4. Chứng minh khả năng chống ghi đè: Giao lại lần 2 ghi đè lần 1, đơn đã Delivered từ chối tin cũ đến muộn.
5. In ra schema bản ghi Parquet bàn giao cho Bronze Lakehouse.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "lambda" / "processor"))

from ddb_writer import OrderWriter, WriteResult  # noqa: E402
from transformer import transform_order  # noqa: E402


class LocalDynamoDBStorage:
    """Mô phỏng bảng DynamoDB thực tế hỗ trợ Conditional Put."""

    def __init__(self) -> None:
        self.items: dict[tuple[str, str], dict] = {}

    def put_item(self, Item: dict, ConditionExpression: str, ExpressionAttributeValues: dict | None = None) -> None:
        key = (Item["pk"], Item["sk"])
        existing = self.items.get(key)

        if existing is not None:
            # 1. Terminal immunity
            if existing.get("is_terminal"):
                raise Exception("ConditionalCheckFailedException: Terminal item cannot be overwritten")

            # 2. Incoming is terminal
            if Item.get("is_terminal"):
                self.items[key] = Item
                return

            # 3. Delivery attempt comparison
            existing_attempt = existing.get("delivery_attempt", 1)
            new_attempt = Item.get("delivery_attempt", 1)
            if new_attempt > existing_attempt:
                self.items[key] = Item
                return
            elif new_attempt < existing_attempt:
                raise Exception("ConditionalCheckFailedException: Stale attempt rejected")

            # 4. Same attempt: phase_rank comparison
            existing_phase = existing.get("phase_rank", 0)
            new_phase = Item.get("phase_rank", 0)
            if new_phase > existing_phase:
                self.items[key] = Item
                return
            elif new_phase < existing_phase:
                raise Exception("ConditionalCheckFailedException: Lower phase rank rejected")

            # 5. Same attempt & phase: updated_at comparison
            new_updated = Item.get("updated_at")
            existing_updated = existing.get("updated_at")
            if existing_updated is None or (new_updated and new_updated > existing_updated):
                self.items[key] = Item
                return

            raise Exception("ConditionalCheckFailedException: Stale timestamp rejected")

        self.items[key] = Item


def run_demo() -> None:
    print("=" * 80)
    print("🚀 CHẠY THỬ NGHIỆM ĐẦY ĐỦ LUỒNG XỬ LÝ CỦA MEMBER 2 (SERVING & DYNAMODB)")
    print("=" * 80)

    storage = LocalDynamoDBStorage()
    writer = OrderWriter(storage)

    order_code = "GHN-DEMO-2026-001"

    # Chuỗi 5 sự kiện thực tế trong vòng đời 1 đơn hàng
    events = [
        {
            "step": "BƯỚC 1: ĐƠN HÀNG MỚI ĐƯỢC TẠO (ready_to_pick)",
            "payload": {
                "order_code": order_code,
                "status": "ready_to_pick",
                "created_at": "2026-11-23T08:00:00Z",
                "updated_at": "2026-11-23T08:00:00Z",
                "to_name": "Nguyễn Văn A",
                "to_phone": "0901234567",
                "to_address": "123 Lê Lợi, Quận 1, TP.HCM",
                "shipping_fee": 35000,
                "cod_amount": 250000,
                "weight": 1.2,
                "log": [{"status": "ready_to_pick", "updated_date": "2026-11-23T08:00:00Z"}],
            },
        },
        {
            "step": "BƯỚC 2: GIAO THẤT BÀI LẦN 1 (delivery_fail - Khách bận)",
            "payload": {
                "order_code": order_code,
                "status": "delivery_fail",
                "created_at": "2026-11-23T08:00:00Z",
                "updated_at": "2026-11-23T11:30:00Z",
                "to_name": "Nguyễn Văn A",
                "to_phone": "0901234567",
                "to_address": "123 Lê Lợi, Quận 1, TP.HCM",
                "shipping_fee": 35000,
                "cod_amount": 250000,
                "weight": 1.2,
                "log": [
                    {"status": "ready_to_pick", "updated_date": "2026-11-23T08:00:00Z"},
                    {"status": "delivering", "updated_date": "2026-11-23T10:00:00Z"},
                    {"status": "delivery_fail", "updated_date": "2026-11-23T11:30:00Z"},
                ],
            },
        },
        {
            "step": "BƯỚC 3: GIAO LẠI LẦN 2 (delivering - Bưu tá đi giao tiếp)",
            "payload": {
                "order_code": order_code,
                "status": "delivering",
                "created_at": "2026-11-23T08:00:00Z",
                "updated_at": "2026-11-23T14:00:00Z",
                "to_name": "Nguyễn Văn A",
                "to_phone": "0901234567",
                "to_address": "123 Lê Lợi, Quận 1, TP.HCM",
                "shipping_fee": 35000,
                "cod_amount": 250000,
                "weight": 1.2,
                "log": [
                    {"status": "ready_to_pick", "updated_date": "2026-11-23T08:00:00Z"},
                    {"status": "delivering", "updated_date": "2026-11-23T10:00:00Z"},
                    {"status": "delivery_fail", "updated_date": "2026-11-23T11:30:00Z"},
                    {"status": "delivering", "updated_date": "2026-11-23T14:00:00Z"},
                ],
            },
        },
        {
            "step": "BƯỚC 4: GIAO THÀNH CÔNG (delivered - Trạng thái kết thúc)",
            "payload": {
                "order_code": order_code,
                "status": "delivered",
                "created_at": "2026-11-23T08:00:00Z",
                "delivered_at": "2026-11-23T14:45:00Z",
                "updated_at": "2026-11-23T14:45:00Z",
                "to_name": "Nguyễn Văn A",
                "to_phone": "0901234567",
                "to_address": "123 Lê Lợi, Quận 1, TP.HCM",
                "shipping_fee": 35000,
                "cod_amount": 250000,
                "weight": 1.2,
                "log": [
                    {"status": "ready_to_pick", "updated_date": "2026-11-23T08:00:00Z"},
                    {"status": "delivering", "updated_date": "2026-11-23T10:00:00Z"},
                    {"status": "delivery_fail", "updated_date": "2026-11-23T11:30:00Z"},
                    {"status": "delivering", "updated_date": "2026-11-23T14:00:00Z"},
                    {"status": "delivered", "updated_date": "2026-11-23T14:45:00Z"},
                ],
            },
        },
        {
            "step": "BƯỚC 5: TIN NHẮN RỚT MẠNG ĐẾN MUỘN (Gửi lại tin 'delivery_fail' lúc 11h30 sau khi đơn đã giao)",
            "payload": {
                "order_code": order_code,
                "status": "delivery_fail",
                "created_at": "2026-11-23T08:00:00Z",
                "updated_at": "2026-11-23T11:30:00Z",
                "to_name": "Nguyễn Văn A",
                "to_phone": "0901234567",
                "to_address": "123 Lê Lợi, Quận 1, TP.HCM",
                "shipping_fee": 35000,
                "cod_amount": 250000,
                "weight": 1.2,
                "log": [
                    {"status": "ready_to_pick", "updated_date": "2026-11-23T08:00:00Z"},
                    {"status": "delivery_fail", "updated_date": "2026-11-23T11:30:00Z"},
                ],
            },
        },
    ]

    for ev in events:
        print(f"\n▶ {ev['step']}")
        raw = ev["payload"]

        # 1. Transform qua Transformer
        transformed = transform_order(raw)

        # 2. Ghi qua DynamoDB Writer
        result = writer.put(transformed)

        if result is WriteResult.WRITTEN:
            print("  ✅ Kết quả ghi: WRITTEN (Đã cập nhật vào DynamoDB)")
        elif result is WriteResult.DUPLICATE:
            print("  🛡️ Kết quả ghi: DUPLICATE (Máy trạng thái chặn lại thành công - No-op, không ghi đè!)")

    # Lấy bản ghi hiện tại trong DynamoDB
    current_item = storage.items[("ORDER#" + order_code, "2026-11-23")]

    print("\n" + "=" * 80)
    print("📊 MẪU DỮ LIỆU BẢNG DYNAMODB HOÀN CHỈNH CHO NHÂN VIÊN CSKH TRA CỨU (< 15ms):")
    print("=" * 80)

    # Hiển thị đẹp các trường CSKH nhìn thấy
    cskh_view = {
        "Khóa tra cứu (PK)": current_item["pk"],
        "Ngày tạo đơn (SK)": current_item["sk"],
        "Mã vận đơn (order_code)": current_item["order_code"],
        "Trạng thái chuẩn hóa (status)": current_item["status"],
        "Đã kết thúc chưa (is_terminal)": current_item["is_terminal"],
        "Lượt giao hàng (delivery_attempt)": current_item["delivery_attempt"],
        "Thứ tự giai đoạn (phase_rank)": current_item["phase_rank"],
        "Thời gian giao hàng (lead_time_hours)": f"{current_item.get('lead_time_hours')} giờ",
        "Tiền thu hộ (COD)": f"{current_item.get('cod_amount'):,} VNĐ" if current_item.get("cod_amount") else "0 VNĐ",
        "Cước vận chuyển (shipping_fee)": f"{current_item.get('shipping_fee'):,} VNĐ" if current_item.get("shipping_fee") else "0 VNĐ",
        "Khối lượng (weight)": f"{current_item.get('weight')} kg",
        "Thời điểm tạo (created_at)": current_item["created_at"],
        "Thời điểm giao thành công (delivered_at)": current_item.get("delivered_at"),
        "Thời điểm cập nhật cuối (updated_at)": current_item["updated_at"],
    }
    for k, v in cskh_view.items():
        print(f"  • {k:40}: {v}")

    print("\n" + "=" * 80)
    print("📦 ĐỊNH DẠNG METADATA CHUẨN BỊ CHO TẦNG BRONZE (PARQUET LAKEHOUSE - MEMBER 3):")
    print("=" * 80)
    bronze_preview = {
        "event_id": "c1f7b028-e4b5-5591-953e-demo0001",
        "order_code": current_item["order_code"],
        "status": current_item["status"],
        "event_type": "status_update",
        "source": "ghn_webhook",
        "event_date": "2026-11-23",
        "event_time": current_item["updated_at"],
        "ingest_date": "2026-11-23",
        "raw_uri": f"s3://ghn-analytics-bronze/raw/2026-11-23_{order_code}.json",
        "parquet_format": "Apache Iceberg v2 / Columnar Parquet / Snappy compressed",
        "partition": "day(ingest_time) = 2026-11-23",
    }
    for k, v in bronze_preview.items():
        print(f"  • {k:25}: {v}")
    print("=" * 80)


if __name__ == "__main__":
    run_demo()
