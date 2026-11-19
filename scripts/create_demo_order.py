#!/usr/bin/env python3
"""Create demo shipping orders for webhook and reconciliation testing.

Usage:
    python3 scripts/create_demo_order.py            # one demo order
    python3 scripts/create_demo_order.py --count 3  # three
    python3 scripts/create_demo_order.py --dry-run   # show the payload only
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "lambda", "ingestor"))

from ghn_client import GhnClient, GhnClientError


def build_payload(index: int, client_order_code: str | None = None) -> dict:
    """A valid create-order payload in GHN's documented field names.

    Address names come from the master-data catalogues (Get Province /
    District / Ward) - these are small, well-known sample values.
    Service type 2 = total weight under 20 kg.
    """
    return {
        # recipient block
        "to_name": f"Người nhận demo {index}",
        "to_phone": "0987654321",
        "to_address": "72 Lê Thánh Tôn, P. Bến Nghé, Quận 1, TP. Hồ Chí Minh",
        "to_ward_name": "Phường Bến Nghé",
        "to_district_name": "Quận 1",
        "to_province_name": "TP. Hồ Chí Minh",
        # sender block (defaults to the shop profile when omitted)
        "from_name": "Shop demo",
        "from_phone": "0332190444",
        "from_address": "39 Nguyễn Thị Thập, P. Tân Phú, Quận 7, TP. Hồ Chí Minh",
        "from_ward_name": "Phường Tân Phú",
        "from_district_name": "Quận 7",
        "from_province_name": "TP. Hồ Chí Minh",
        # parcel + service
        "weight": 600,  # grams
        "length": 25,
        "width": 20,
        "height": 8,
        "service_type_id": 2,
        "payment_type_id": 1,
        "required_note": "CHOTHUHANG",
        "content": f"Demo order {index} - test parcel",
        # idempotency: re-running never creates a duplicate for the same
        # client_order_code; GHN returns the previously created code.
        "client_order_code": client_order_code or f"DEMO-{int(time.time())}-{index}-{uuid.uuid4().hex[:6]}",
        "cod_amount": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1, help="how many orders to create")
    parser.add_argument("--dry-run", action="store_true", help="print the payload without calling the API")
    parser.add_argument("--client-order-code", help="reuse this exact code when retrying one order")
    parser.add_argument(
        "--host",
        default=None,
        help="override GHN_BASE_URL (staging tokens need https://dev-online-gateway.ghn.vn)",
    )
    args = parser.parse_args()

    if args.count < 1 or (args.client_order_code and args.count != 1):
        parser.error("--count must be positive; --client-order-code requires --count 1")

    if args.dry_run:
        print(json.dumps(build_payload(1, args.client_order_code), ensure_ascii=True, indent=2))
        return 0

    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    base = args.host or os.environ.get("GHN_BASE_URL", "")
    if base.rstrip("/") != "https://dev-online-gateway.ghn.vn":
        print("ERROR: create_demo_order only permits the GHN staging gateway", file=sys.stderr)
        return 1
    if not token or not shop:
        print("ERROR: set GHN_API_TOKEN and GHN_SHOP_ID in .env", file=sys.stderr)
        return 1
    print(f"target: {base} (shop {shop})")

    client = GhnClient(token=token, base_url=base, shop_id=shop)
    created: list[str] = []
    records: list[dict[str, str]] = []
    for i in range(1, args.count + 1):
        payload = build_payload(i, args.client_order_code)
        try:
            data = client.create_order(payload)
        except GhnClientError as exc:
            print(f"[{i}] failed for client_order_code={payload['client_order_code']}: {exc}", file=sys.stderr)
            continue
        code = data.get("order_code", "?")
        created.append(code)
        records.append({"order_code": code, "client_order_code": payload["client_order_code"], "created_request_at": datetime.now(UTC).isoformat()})
        print(f"[{i}] order_code={code} client_order_code={payload['client_order_code']} fee={data.get('total_fee')}")

    if not created:
        return 1

    output = Path(__file__).resolve().parents[1] / ".local_staging_orders.jsonl"
    with output.open("a", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"Order codes saved to {output.name}")

    # Let reconciliation find these test orders even if a callback is missed.
    table_name = os.environ.get("LOCAL_WATCHLIST_TABLE_NAME", "ghn-analytics-local-watchlist")
    registered = False
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
    if endpoint.startswith(("http://localhost:", "http://127.0.0.1:")):
        try:
            from watchlist import register_order

            os.environ["AWS_ENDPOINT_URL"] = endpoint
            os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-1"
            os.environ["AWS_ACCESS_KEY_ID"] = "test"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
            os.environ["ORDER_WATCHLIST_TABLE"] = table_name
            for code in created:
                register_order(code)
            registered = True
            print(f"registered {len(created)} code(s) in {table_name} for reconciliation.")
        except Exception as exc:  # pragma: no cover - depends on AWS creds
            print(f"warning: could not register in {table_name} ({exc})", file=sys.stderr)

    print()
    if not registered:
        print("Watchlist registration unavailable. An accepted webhook will register the order.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
