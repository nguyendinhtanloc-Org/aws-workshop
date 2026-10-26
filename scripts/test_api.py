#!/usr/bin/env python3
"""Probe the GHN API with your token - no AWS account needed.

The fastest way to check that your token + ShopId work and to see the
real payloads, before deploying anything. Uses the same client methods
the Ingestor does, so what you see here is what the pipeline will see.

Usage:
    python3 scripts/test_api.py                 # master data + watched orders
    python3 scripts/test_api.py --endpoint master_data
    python3 scripts/test_api.py --endpoint orders --limit 3
    GHN_MOCK=1 python3 scripts/test_api.py       # offline, generated data

Read-only: it never writes to S3, Kinesis or DynamoDB, and it does not
call the create-order route unless you ask (--create).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "lambda", "ingestor"))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from ghn_client import GhnClient, GhnClientError  # noqa: E402
from mock_source import MockGhnClient  # noqa: E402


def _csv(value: str) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _make_client() -> object:
    if os.environ.get("GHN_MOCK", "").lower() in ("1", "true", "yes"):
        print("[GHN_MOCK=1 - generated data, not calling the GHN API]")
        return MockGhnClient()
    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    base = os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn")
    if not shop:
        print("[note] GHN_SHOP_ID is blank - the gateway requires it. Set it in .env.")
    return GhnClient(token=token, base_url=base, shop_id=shop)


def _show_records(label: str, records: list[dict], limit: int) -> int:
    print(f"\n=== {label}: {len(records)} record(s) ===")
    for record in records[:limit]:
        print("  " + json.dumps(record, ensure_ascii=False, indent=2).replace("\n", "\n  "))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", choices=["master_data", "orders", "all"], default="all")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument(
        "--create", action="store_true",
        help="also call the create-order route once, to show its payload/fee",
    )
    args = parser.parse_args()

    client = _make_client()
    failures = 0

    if args.endpoint in ("master_data", "all"):
        ward_districts = _csv(os.environ.get("GHN_WARD_DISTRICTS", ""))
        try:
            records = client.fetch_master_data(ward_district_ids=ward_districts or None)
            failures += _show_records("master_data", records, args.limit)
        except GhnClientError as exc:
            print(f"\n=== master_data FAILED: {exc}")
            failures += 1

    if args.endpoint in ("orders", "all"):
        codes = _csv(os.environ.get("GHN_ORDER_CODES", ""))
        if not codes and not getattr(client, "_rng", None):
            print("\n[orders] GHN_ORDER_CODES is empty and the API has no list-orders "
                  "route - nothing to fetch. Create one first:")
            print("    python3 scripts/create_demo_order.py")
        else:
            try:
                records = client.fetch_orders(order_codes=codes or None)
                failures += _show_records("orders", records, args.limit)
            except GhnClientError as exc:
                print(f"\n=== orders FAILED: {exc}")
                failures += 1

    if args.create:
        from create_demo_order import build_payload  # noqa: E402  (same dir)
        try:
            data = client.create_order(build_payload(1))
            print(f"\n=== create_order: order_code={data.get('order_code')} "
                  f"total_fee={data.get('total_fee')} eta={data.get('expected_delivery_time')}")
        except GhnClientError as exc:
            print(f"\n=== create_order FAILED: {exc}")
            failures += 1

    print()
    if failures:
        print(f"{failures} target(s) failed - check your token, ShopId and watchlists.")
        return 1
    print("All targets responded successfully.")
    return 0


if __name__ == "__main__":
    # importable from its own directory for --create's helper
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.exit(main())
