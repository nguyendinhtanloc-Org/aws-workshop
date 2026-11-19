#!/usr/bin/env python3
"""Update a GHN staging order.

Examples:
    python scripts/update_demo_order.py --order-code CODE --note "Updated note"
    python scripts/update_demo_order.py --order-code CODE --weight 700 --dry-run
    python scripts/update_demo_order.py --order-code CODE --payload changes.json

The payload file is a JSON object containing fields to change (UTF-8).
Allowed fields depend on the order status; changing COD may require an OTP.
API docs: https://developer.ghn.vn/en/docs/order/update
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
from ghn_client import STAGING_BASE_URL, GhnClient, GhnClientError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-code", required=True, help="GHN order_code, not client_order_code")
    parser.add_argument("--payload", type=Path, help="JSON file with fields to update")
    parser.add_argument("--note", help="new delivery note")
    parser.add_argument("--weight", type=int, help="new weight in grams")
    parser.add_argument("--host", help="override GHN_BASE_URL (staging only)")
    parser.add_argument("--dry-run", action="store_true", help="show payload without calling GHN")
    args = parser.parse_args()
    payload = {}
    if args.payload:
        try:
            payload = json.loads(args.payload.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as exc:
            parser.error(f"cannot read payload: {exc}")
        if not isinstance(payload, dict):
            parser.error("payload must be a JSON object")
    code = args.order_code.strip()
    if not code:
        parser.error("--order-code cannot be empty")
    if "order_code" in payload and payload["order_code"] != code:
        parser.error("payload order_code conflicts with --order-code")
    payload["order_code"] = code
    if args.note is not None:
        payload["note"] = args.note
    if args.weight is not None:
        if args.weight <= 0:
            parser.error("--weight must be positive")
        payload["weight"] = args.weight
    if len(payload) == 1:
        parser.error("provide --note, --weight or --payload with fields to change")
    if args.dry_run:
        # OTP is a credential: never print it in previews.
        preview = {**payload}
        if "otp" in preview:
            preview["otp"] = "[REDACTED]"
        print(json.dumps(preview, ensure_ascii=True, indent=2))
        return 0
    base = (args.host or os.environ.get("GHN_BASE_URL", STAGING_BASE_URL)).rstrip("/")
    if base != STAGING_BASE_URL:
        parser.error("only the GHN staging gateway is permitted")
    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    if not token or not shop:
        parser.error("set GHN_API_TOKEN and GHN_SHOP_ID in .env")
    try:
        response = GhnClient(token=token, base_url=base, shop_id=shop).update_order(payload)
    except (GhnClientError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(response, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
