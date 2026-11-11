#!/usr/bin/env python3
"""Find the exact query-param name the shipping-order/detail route expects.

The gateway's 400 message names the Go struct field (OrderCode), not
the query-param name. The ward route accepted the snake_case
district_id, so the detail route probably wants order_code too.
This tries each candidate name against the same order code and
reports which one returns data instead of a 400.

Usage:
    python3 scripts/probe_order_detail_params.py
    python3 scripts/probe_order_detail_params.py --code LYQGYR -v
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys

import requests

with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv()

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "lambda", "ingestor"))

from ghn_client import API_PREFIX, ENDPOINTS  # noqa: E402

# Candidate query-param names, most-likely first.
CANDIDATES = [
    "order_code",
    "OrderCode",
    "orderCode",
    "shipping_order_code",
    "customer_order_code",
    "ClientOrderCode",
    "client_order_code",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code", default="", help="order code to look up (default: first GHN_ORDER_CODES entry)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    base = os.environ.get("GHN_BASE_URL", "https://dev-online-gateway.ghn.vn").rstrip("/")
    code = args.code or next((c.strip() for c in os.environ.get("GHN_ORDER_CODES", "").split(",") if c.strip()), "")
    if not token or not code:
        sys.exit("ERROR: set GHN_API_TOKEN and GHN_ORDER_CODES in .env (or pass --code)")

    url = f"{base}{API_PREFIX}{ENDPOINTS['orders']}"
    headers = {"Token": token}
    if shop:
        headers["ShopId"] = shop

    print(f"order code: {code}\nurl: {url}")
    found = False
    for name in CANDIDATES:
        resp = requests.get(url, headers=headers, params={name: code}, timeout=12)
        try:
            body = resp.json()
            message = body.get("message", "")
        except ValueError:
            message = resp.text[:120]
        if resp.status_code == 200 and body.get("code") in (200, None):
            mark = "   <== WORKS"
            found = True
            print(f"  ?{name}={code}  HTTP {resp.status_code} {mark}")
            if args.verbose or True:
                data = body.get("data") or {}
                print("    " + json.dumps(data, ensure_ascii=False)[:400])
        else:
            print(f"  ?{name}={code}  HTTP {resp.status_code} {str(message)[:80]}")
        if found:
            print(f"\nPin {name!r} as the detail-route param in ghn_client.py.")
            break
    return 0 if found else 1


if __name__ == "__main__":
    sys.exit(main())
