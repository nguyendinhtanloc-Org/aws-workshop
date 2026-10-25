#!/usr/bin/env python3
"""Probe the GHN API with your token - no AWS account needed.

This is the fastest way to find out whether your token works and what the
real payloads look like, before deploying anything.

Usage:
    export GHN_API_TOKEN=your_token_here
    python3 scripts/test_api.py

    # inspect a specific endpoint
    python3 scripts/test_api.py --endpoint /api/v1/apiv3/getOrder --limit 3

The script only reads. It never writes to S3, Kinesis or DynamoDB.
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

from ghn_client import ENDPOINTS, GhnClient, GhnClientError  # noqa: E402
from mock_source import MockGhnClient  # noqa: E402

# getTransportCost takes a POST body (from/to address, weight), so it is
# not probed by this GET-only script. The Ingestor does not use it today;
# shipping_fee comes from the order record itself.


def _token() -> str:
    token = os.environ.get("GHN_API_TOKEN")
    if not token:
        sys.exit(
            "No token found.\n"
            "  export GHN_API_TOKEN=your_token_here\n"
            "  (or put it in a .env file next to .env.example)"
        )
    return token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--endpoint",
        choices=[*ENDPOINTS, "all"],
        default="all",
        help="which endpoint to probe (default: all)",
    )
    parser.add_argument("--limit", type=int, default=2, help="records to print per endpoint")
    args = parser.parse_args()

    shop_id = os.environ.get("GHN_SHOP_ID", "")
    partner_id = os.environ.get("GHN_PARTNER_ID", "")
    base_url = os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn")

    if os.environ.get("GHN_MOCK", "").lower() in ("1", "true", "yes"):
        print("[GHN_MOCK=1 - showing generated data, not calling the GHN API]")
        client = MockGhnClient()
    else:
        if not partner_id:
            print("[note] GHN_PARTNER_ID is blank - the gateway will 404 "
                  "every route without it. Set it in .env first.")
        client = GhnClient(
            token=_token(),
            base_url=base_url,
            shop_id=shop_id,
            partner_id=partner_id,
        )
    targets = ENDPOINTS if args.endpoint == "all" else {args.endpoint: ENDPOINTS[args.endpoint]}

    failures = 0
    for name, path in targets.items():
        print(f"\n=== {name}  ({path}) ===")
        try:
            payload = client._get(path)  # noqa: SLF001 - deliberate probe
        except GhnClientError as exc:
            print(f"  FAILED: {exc}")
            failures += 1
            continue

        data = payload.get("data")
        if isinstance(data, list):
            print(f"  {len(data)} record(s)")
            for record in data[: args.limit]:
                print("  " + json.dumps(record, ensure_ascii=False, indent=2).replace("\n", "\n  "))
        else:
            print("  " + json.dumps(payload, ensure_ascii=False, indent=2)[:2000])

    if failures:
        print(f"\n{failures} endpoint(s) failed - check your token and permissions.")
        return 1
    print("\nAll endpoints responded successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
