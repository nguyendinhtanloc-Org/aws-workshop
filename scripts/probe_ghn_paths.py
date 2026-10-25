#!/usr/bin/env python3
"""Find the working GHN gateway base URL, partner id and endpoint paths.

GHN's current open API (per the docs' "API Environments" table) lives on
the bare gateway host:

    production  https://online-gateway.ghn.vn
    staging     https://dev-online-gateway.ghn.vn

Requests carry two auth headers - Token and ShopId - and the partner id
is a PATH segment:

    {gateway}/shiip/public-api/{partner_id}{endpoint}

Without the partner id the gateway answers every route with
{"code":404}, which is the routing 404 that every earlier probe
produced. This script tries the candidate combinations read-only and
prints which one returns data, so ghn_client.py's ENDPOINTS and the
GHN_PARTNER_ID / GHN_SHOP_ID env vars can be pinned.

Usage:
    python3 scripts/probe_ghn_paths.py            # the full matrix
    python3 scripts/probe_ghn_paths.py --spec     # fetch the OpenAPI spec
    python3 scripts/probe_ghn_paths.py -v         # show a sample record
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import requests

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "lambda", "ingestor"))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

HOSTS = [
    "https://online-gateway.ghn.vn",
    "https://dev-online-gateway.ghn.vn",
]
PREFIX = "/shiip/public-api"
ENDPOINT_PATHS = [
    "/client/api/v4/provinces",
    "/client/api/v4/orders",
    "/client/api/v4/districts",
    "/client/api/v4/wards",
]
SPEC_PATHS = ["/swagger.json", "/openapi.json", "/api-docs", f"{PREFIX}/swagger.json"]


def _creds() -> tuple[str, str]:
    token = os.environ.get("GHN_API_TOKEN", "")
    shop = os.environ.get("GHN_SHOP_ID", "")
    if not token:
        sys.exit("ERROR: set GHN_API_TOKEN (or put it in .env)")
    return token, shop


def _headers(token: str, shop: str) -> dict[str, str]:
    h = {"Token": token, "Content-Type": "application/json"}
    if shop:
        h["ShopId"] = shop
    return h


def _call(url: str, headers: dict[str, str]) -> tuple[int, str, bool]:
    try:
        r = requests.get(url, headers=headers, timeout=12)
    except requests.RequestException as exc:
        return 0, f"UNREACHABLE ({type(exc).__name__})", False
    try:
        payload = r.json()
    except ValueError:
        return r.status_code, r.text[:90].replace("\n", " "), False
    code = payload.get("code")
    usable = r.status_code == 200 and code in (200, None)
    data = payload.get("data")
    rows = len(data) if isinstance(data, list) else (1 if data else 0)
    return r.status_code, f"code={code} rows={rows}", usable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partner", default=os.environ.get("GHN_PARTNER_ID", ""),
                        help="partner id path segment (also read from GHN_PARTNER_ID / .env)")
    parser.add_argument("-v", "--verbose", action="store_true", help="print a sample record")
    parser.add_argument("--spec", action="store_true", help="fetch the OpenAPI/swagger spec")
    parser.add_argument("--host", default=None, help="override the gateway host")
    args = parser.parse_args()

    token, shop = _creds()
    hosts = [args.host] if args.host else HOSTS
    partners = [args.partner] if args.partner else [args.partner, ""]

    if args.spec:
        print("=== looking for the OpenAPI / swagger spec ===")
        for host in hosts:
            for sp in SPEC_PATHS:
                url = f"{host}{sp}"
                status, snippet, _ = _call(url, _headers(token, shop))
                print(f"  {url}\n    HTTP {status} {snippet[:100]}")
                if "swagger" in snippet.lower() or "openapi" in snippet.lower():
                    print(f"  -> spec found at {url}; it lists the real route paths")
        print()

    print("=== endpoint matrix (partner id x path) ===")
    hits: list[str] = []
    for host in hosts:
        for partner in partners:
            for path in ENDPOINT_PATHS:
                url = f"{host}{PREFIX}/{partner}{path}" if partner else f"{host}{PREFIX}{path}"
                status, detail, usable = _call(url, _headers(token, shop))
                mark = "  <== WORKS" if usable else ""
                print(f"  host={host.split('//')[1]} partner={partner or '(none)'} {path}  HTTP {status} {detail}{mark}")
                if usable:
                    hits.append(url)
                    if args.verbose:
                        r = requests.get(url, headers=_headers(token, shop), timeout=12)
                        d = r.json().get("data")
                        sample = (d[0] if isinstance(d, list) and d else d)
                        print("    sample:", json.dumps(sample, ensure_ascii=False)[:200])

    print("\n" + "=" * 70)
    if hits:
        print("Working URLs:")
        for h in hits:
            print("  " + h)
        print("\nPin these: GHN_PARTNER_ID=<segment>, GHN_SHOP_ID=<id> in .env, and the")
        print("confirmed paths into ghn_client.py's ENDPOINTS dict.")
    else:
        print("No combination returned data. Likely causes:")
        print("  - GHN_SHOP_ID is blank but the gateway requires it. Set it in .env.")
        print("  - GHN_PARTNER_ID is blank but the gateway needs it as a path segment.")
        print("    It usually appears in your GHN partner dashboard / API keys page.")
        print("  - The ENDPOINT_PATHS above are guesses; run with --spec to pull the real")
        print("    route list from the gateway's OpenAPI spec.")
    return 0 if hits else 1


if __name__ == "__main__":
    sys.exit(main())
