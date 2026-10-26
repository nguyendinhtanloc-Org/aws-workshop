#!/usr/bin/env python3
"""Create demo shipping orders through the GHN API, for the Ingestor to poll.

The current GHN API has no list-orders route, so a shop's order codes
enter the pipeline by being created here (or by your own shop's sales).
Each created code should be appended to GHN_ORDER_CODES so the scheduled
Ingestor run picks it up.

Usage:
    python3 scripts/create_demo_order.py            # one demo order
    python3 scripts/create_demo_order.py --count 3  # three
    python3 scripts/create_demo_order.py --dry-run   # show the payload only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "lambda", "ingestor"))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from ghn_client import GhnClient, GhnClientError  # noqa: E402


def build_payload(index: int) -> dict:
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
        "client_order_code": f"DEMO-{int(time.time())}-{index}",
        "cod_amount": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1, help="how many orders to create")
    parser.add_argument("--dry-run", action="store_true", help="print the payload without calling the API")
    args = parser.parse_args()

    if args.dry_run:
        print(json.dumps(build_payload(1), ensure_ascii=False, indent=2))
        return 0

    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    base = os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn")
    if not token or not shop:
        print("ERROR: set GHN_API_TOKEN and GHN_SHOP_ID in .env", file=sys.stderr)
        return 1

    client = GhnClient(token=token, base_url=base, shop_id=shop)
    created: list[str] = []
    for i in range(1, args.count + 1):
        try:
            data = client.create_order(build_payload(i))
        except GhnClientError as exc:
            print(f"[{i}] failed: {exc}", file=sys.stderr)
            continue
        code = data.get("order_code", "?")
        created.append(code)
        print(f"[{i}] created {code}  fee={data.get('total_fee')}  eta={data.get('expected_delivery_time')}")

    if not created:
        return 1
    print()
    print("Append these to GHN_ORDER_CODES in .env:")
    print(f"  GHN_ORDER_CODES={','.join(created)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
