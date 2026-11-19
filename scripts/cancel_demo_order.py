#!/usr/bin/env python3
"""Cancel GHN staging orders. Cancelled orders remain in GHN history.

Examples:
    python scripts/cancel_demo_order.py --order-codes CODE
    python scripts/cancel_demo_order.py --order-codes CODE1 CODE2 --dry-run
    python scripts/cancel_demo_order.py --order-codes CODE --reason "Demo completed"

API docs: https://developer.ghn.vn/en/docs/order/cancel
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")

sys.path.insert(0, str(ROOT / "src" / "lambda" / "ingestor"))
from ghn_client import STAGING_BASE_URL, GhnClient, GhnClientError


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-codes", nargs="+", required=True, help="GHN order_code values")
    parser.add_argument("--reason", help="cancellation note")
    parser.add_argument("--reason-code", choices=["GHN-CO001", "GHN-CO002", "GHN-CO003", "GHN-CANCEL-OTHER"])
    parser.add_argument("--host", help="override GHN_BASE_URL (staging only)")
    parser.add_argument("--dry-run", action="store_true", help="show payload without calling GHN")
    args = parser.parse_args()
    if any(not code.strip() for code in args.order_codes):
        parser.error("order codes cannot be empty")
    codes = list(dict.fromkeys(code.strip() for code in args.order_codes))
    payload = {"order_codes": codes}
    if args.reason is not None:
        payload["reason"] = args.reason
    if args.reason_code is not None:
        payload["reason_code"] = args.reason_code
    if args.dry_run:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    base = (args.host or os.environ.get("GHN_BASE_URL", STAGING_BASE_URL)).rstrip("/")
    if base != STAGING_BASE_URL:
        parser.error("only the GHN staging gateway is permitted")
    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    if not token or not shop:
        parser.error("set GHN_API_TOKEN and GHN_SHOP_ID in .env")
    try:
        response = GhnClient(token=token, base_url=base, shop_id=shop).cancel_orders(payload)
    except (GhnClientError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(response, ensure_ascii=True, indent=2))
    results = response.get("data")
    if not isinstance(results, list) or any(
        not any(isinstance(row, dict) and row.get("order_code") == code
                and row.get("result") is True for row in results)
        for code in codes
    ):
        print("ERROR: GHN did not confirm cancellation for every requested order", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
