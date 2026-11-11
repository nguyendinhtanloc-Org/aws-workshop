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
import contextlib
import json
import os
import sys
import time
from datetime import UTC, datetime

with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "lambda", "ingestor"))

from ghn_client import GhnClient, GhnClientError


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
    parser.add_argument(
        "--host",
        default=None,
        help="override GHN_BASE_URL (staging tokens need https://dev-online-gateway.ghn.vn)",
    )
    args = parser.parse_args()

    if args.dry_run:
        print(json.dumps(build_payload(1), ensure_ascii=False, indent=2))
        return 0

    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    base = args.host or os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn")
    if not token or not shop:
        print("ERROR: set GHN_API_TOKEN and GHN_SHOP_ID in .env", file=sys.stderr)
        return 1
    print(f"target: {base} (shop {shop}) - staging tokens need the dev gateway")

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

    # Preferred path: register the new codes directly in the DynamoDB
    # order-watchlist table (the Ingestor polls it and self-maintains
    # terminal-state rows, so no redeploy is ever needed for new orders).
    table_name = os.environ.get("ORDER_WATCHLIST_TABLE", "")
    registered = False
    if table_name:
        try:
            import boto3

            ddb = boto3.resource("dynamodb")
            table = ddb.Table(table_name)
            now = datetime.now(UTC).isoformat()
            with table.batch_writer() as batch:
                for code in created:
                    batch.put_item(
                        Item={
                            "pk": "#WATCH",
                            "sk": code,
                            "terminal": False,
                            "registered_at": now,
                            "updated_at": now,
                        }
                    )
            registered = True
            print(f"registered {len(created)} code(s) in {table_name} - the Ingestor picks them up on its next run.")
        except Exception as exc:  # pragma: no cover - depends on AWS creds
            print(f"warning: could not register in {table_name} ({exc}); fall back to .env below", file=sys.stderr)

    print()
    if not registered:
        print("No watchlist table available. Append these to GHN_ORDER_CODES in .env instead:")
        print(f"  GHN_ORDER_CODES={','.join(created)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
