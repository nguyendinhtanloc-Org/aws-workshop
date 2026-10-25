#!/usr/bin/env python3
"""Find the working GHN gateway endpoint paths.

GHN's current open API (per the docs' "API Environments" table) lives on
the bare gateway host:

    production  https://online-gateway.ghn.vn
    staging     https://dev-online-gateway.ghn.vn

Requests are authenticated with two headers - Token (API key) and ShopId.
The exact endpoint paths for the current generation are not known yet, so
this script probes a matrix of candidate prefixes x endpoint names and
reports which ones answer with data, so ghn_client.py's ENDPOINTS and
GHN_API_PREFIX can be pinned to the working set.

Reading the results:

- Every route answers 401 "Token is not valid!" -> the token is not
  accepted by this environment; fix the credentials first, the matrix
  result is meaningless.
- Auth passes and a route answers 200 -> that URL is a confirmed
  endpoint; pin it.
- Auth passes and a route answers 404 -> the gateway has no such route;
  the route names still need to come from the GHN docs' "Open
  endpoint" pages, which list the full URL template.

Usage:
    python3 scripts/probe_ghn_paths.py --host https://dev-online-gateway.ghn.vn
    python3 scripts/probe_ghn_paths.py --host https://dev-online-gateway.ghn.vn -v
    python3 scripts/probe_ghn_paths.py --prefix '' --host ...   # skip the full matrix
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

# Candidate prefixes: the API may sit directly on the host, under
# /shiip/public-api, or under another mount.
PREFIXES = [
    "/shipecode/api",          # GHN public-API mount seen in merchant docs
    "/shipecode",
    "",
    "/shiip/public-api",
    "/public-api",
]

# Candidate route names: REST-style current generation, legacy camelCase
# names, and both v3/v4. With a valid token, 404 means the route does not
# exist at that URL and 200 means it does - the first 200 is confirmed.
# The routes confirmed from the GHN docs (developer.ghn.dev):
CONFIRMED_PATHS = [
    "/master-data/province",
    "/master-data/district",
    "/master-data/ward",
    "/v2/shipping-order/detail",
]
# Legacy guesses, kept as a fallback matrix when the confirmed routes
# unexpectedly 404.
LEGACY_PATHS = [
    "/client/api/v4/provinces",
    "/client/api/v4/orders",
    "/api/v1/apiv4/getProvinces",
    "/api/v1/apiv3/getProvinces",
    "/provinces",
]
ENDPOINT_PATHS = CONFIRMED_PATHS + LEGACY_PATHS


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
    if r.status_code == 401:
        message = payload.get("message") or r.text[:120].replace("\n", " ")
        detail = f"code={code} msg={message!r}"
    return r.status_code, detail, usable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="print a sample record")
    parser.add_argument("--host", default=None, help="override the gateway host")
    parser.add_argument(
        "--prefix", default="",
        help="probe a single specific prefix instead of the full matrix",
    )
    args = parser.parse_args()

    token, shop = _creds()
    hosts = [args.host] if args.host else HOSTS

    if not shop:
        print("[note] GHN_SHOP_ID is blank. The current gateway requires the "
              "ShopId header; every response may be a routing 404 until it is set.\n")

    prefixes = [args.prefix] if args.prefix else PREFIXES
    print(f"=== endpoint matrix: {len(hosts)} host(s) x {len(prefixes)} prefix(es) x {len(ENDPOINT_PATHS)} path(s) ===")
    hits: list[str] = []
    any401 = False
    for host in hosts:
        for prefix in prefixes:
            for path in ENDPOINT_PATHS:
                url = f"{host}{prefix}{path}"
                status, detail, usable = _call(url, _headers(token, shop))
                if usable:
                    mark = "  <== WORKS"
                elif status == 401:
                    mark = "  <== auth failing"
                elif status == 405:
                    # 405 = the route EXISTS but does not accept GET -
                    # it needs a POST/PUT body. This is the strongest
                    # signal that we are at the right place.
                    mark = "  <== route exists, wrong method (try POST)"
                else:
                    mark = ""
                print(f"  host={host.split('//')[1]:32} prefix={prefix or '(none)':18} {path}  HTTP {status} {mark}")
                if usable:
                    hits.append(url)
                if status == 401:
                    any401 = True
                if args.verbose and usable:
                    r = requests.get(url, headers=_headers(token, shop), timeout=12)
                    d = r.json().get("data")
                    sample = (d[0] if isinstance(d, list) and d else d)
                    print("    sample:", json.dumps(sample, ensure_ascii=False)[:200])

    print("\n" + "=" * 70)
    if hits:
        print("Working URLs:")
        for h in hits:
            print("  " + h)
        print("\nPin the confirmed prefix + paths into ghn_client.py (API_PREFIX and")
        print("ENDPOINTS) and GHN_API_PREFIX / GHN_SHOP_ID in .env.")
    elif any401:
        print("Every route answered 401 (\"Token is not valid\"). The token is not")
        print("accepted in this environment. Get a key issued for THIS gateway,")
        print("then re-run - the 404s that follow will be about route names.")
    else:
        print("Auth passed but no GET route returned data. Two next moves:")
        print("  1. Any HTTP 405 above means that exact path exists and needs a")
        print("     POST/PUT body - pin it into ghn_client.py and switch the verb.")
        print("  2. Open one GHN docs \"Open endpoint\" page and copy the full URL")
        print("     template (host + mount + path, including any path parameters) -")
        print("     it is the authoritative answer and beats this search loop.")
    return 0 if hits else 1


if __name__ == "__main__":
    sys.exit(main())
