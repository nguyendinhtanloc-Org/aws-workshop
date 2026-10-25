#!/usr/bin/env python3
"""Find the working GHN gateway endpoint paths.

GHN's current open API (per the docs' "API Environments" table) lives on
the bare gateway host:

    production  https://online-gateway.ghn.vn
    staging     https://dev-online-gateway.ghn.vn

Requests are authenticated with two headers - Token (API key) and ShopId.
The exact endpoint paths for the current generation are not known yet, so
this script probes the candidate paths read-only and reports which ones
answer with data, so ghn_client.py's ENDPOINTS can be pinned to the
working set.

The OpenAPI spec is the authoritative source for the route list; when the
gateway exposes one, run with --spec to read it.

Usage:
    python3 scripts/probe_ghn_paths.py           # the path matrix
    python3 scripts/probe_ghn_paths.py --spec    # fetch the OpenAPI spec
    python3 scripts/probe_ghn_paths.py -v        # show a sample record
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

# GHN_API_PREFIX is the path prefix the gateway mounts the API under.
# Overridable because GHN has moved it between generations.
DEFAULT_PREFIX = os.environ.get("GHN_API_PREFIX", "/shiip/public-api")

ENDPOINT_PATHS = [
    "/client/api/v4/provinces",
    "/client/api/v4/orders",
    "/client/api/v4/districts",
    "/client/api/v4/wards",
    "/api/v1/apiv4/provinces",
    "/api/v1/apiv4/orders",
]
SPEC_PATHS = ["/swagger.json", "/openapi.json", f"{DEFAULT_PREFIX}/swagger.json", f"{DEFAULT_PREFIX}/openapi.json"]


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
    detail = f"code={code} rows={rows}"
    # On a 401 the gateway's own message names the reason ("invalid
    # token", "shop not registered", ...) - print it plus any auth
    # challenge headers, since that is what decides the next step.
    if r.status_code == 401:
        message = payload.get("message") or r.text[:120].replace("\n", " ")
        auth = {k: v for k, v in r.headers.items() if "auth" in k.lower()}
        detail = f"code={code} msg={message!r} {auth or ''}"
    return r.status_code, detail, usable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="print a sample record")
    parser.add_argument("--spec", action="store_true", help="fetch the OpenAPI/swagger spec")
    parser.add_argument("--host", default=None, help="override the gateway host")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="override the API path prefix")
    args = parser.parse_args()

    token, shop = _creds()
    hosts = [args.host] if args.host else HOSTS

    if not shop:
        print("[note] GHN_SHOP_ID is blank. The current gateway requires the "
              "ShopId header; every response below may be a routing 404 "
              "until it is set. Get it from your GHN partner dashboard.\n")

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

    print(f"=== endpoint matrix (host x prefix {args.prefix} x path) ===")
    hits: list[str] = []
    any401 = False
    for host in hosts:
        for path in ENDPOINT_PATHS:
            url = f"{host}{args.prefix}{path}"
            status, detail, usable = _call(url, _headers(token, shop))
            if usable:
                mark = "  <== WORKS"
            elif status == 401:
                # A 401 means the router found the route and rejected the
                # credentials - the URL shape is right, the auth is not.
                mark = "  <== route exists, auth failing (Token/ShopId)"
            else:
                mark = ""
            print(f"  host={host.split('//')[1]} {path}  HTTP {status} {detail}{mark}")
            if usable:
                hits.append(url)
            if status == 401:
                any401 = True
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
        print("\nPin the confirmed paths into ghn_client.py's ENDPOINTS dict, and")
        print("GHN_SHOP_ID (and GHN_API_PREFIX if different) in .env.")
    elif any401:
        print("Every route answered 401: the URL shape is right, the credentials")
        print("are what's failing. Set GHN_SHOP_ID in .env, and check whether your")
        print("API key was issued for this environment (dev tokens do not work")
        print("against the production gateway, and vice versa).")
    else:
        print("No path returned data. Likely causes, in order of probability:")
        print("  1. GHN_SHOP_ID is blank - set it; the router needs that header.")
        print("  2. The GHN_API_PREFIX is wrong for this generation - try it with")
        print("     --prefix '' (no prefix) or another prefix, and check the")
        print("     endpoint URLs in the GHN docs' 'Open endpoint' pages.")
        print("  3. The endpoint paths above are guesses; run with --spec to pull")
        print("     the real route list from the gateway's OpenAPI spec.")
    return 0 if hits else 1


if __name__ == "__main__":
    sys.exit(main())
