#!/usr/bin/env python3
"""Find which GHN base URL and endpoint paths actually work.

The GHN open API has changed its host and path layout between
generations. Every response here comes back as JSON
{"code":404,"message":"Not Found"}, which is the gateway rejecting the
route - so we probe a matrix of base prefixes against a matrix of
endpoint paths and report anything that answers with data.

Read-only. Makes no writes anywhere.

Usage:
    python3 scripts/probe_ghn_paths.py          # the full matrix
    python3 scripts/probe_ghn_paths.py -v       # show the first record
    python3 scripts/probe_ghn_paths.py --quick  # one prefix only
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

# Candidate base prefixes. Ordered most-likely first so a quick run is
# still useful.
HOST = "https://online-gateway.ghn.vn"
PREFIXES = [
    "/shiip/public-api",
    "/shiip/public-api/",
    "",
    "/public-api",
    "/shiip",
    "/gateway",
    "/api",
]

# Candidate endpoint paths, tried against every prefix.
PATHS = [
    "/api/v1/apiv4/getProvinces",
    "/api/v1/apiv3/getProvinces",
    "/v1/api/apiv4/getProvinces",
    "/api/v1/apiv4/client/orders",
    "/apiv4/getProvinces",
    "/getProvinces",
    "/swagger.json",
    "/openapi.json",
    "/",
]


def _token() -> str:
    token = os.environ.get("GHN_API_TOKEN")
    if not token:
        sys.exit("ERROR: set GHN_API_TOKEN (or put it in .env)")
    return token


def _probe(url: str, headers: dict[str, str], method: str = "GET") -> tuple[int, str, bool]:
    """Return (status, snippet, usable)."""
    try:
        r = requests.request(method, url, headers=headers, timeout=10)
    except requests.RequestException as exc:
        return 0, f"UNREACHABLE ({type(exc).__name__})", False
    snippet = r.text[:140].replace("\n", " ")
    try:
        payload = r.json()
    except ValueError:
        return r.status_code, snippet, False
    code = payload.get("code")
    usable = r.status_code == 200 and code in (200, None)
    return r.status_code, f"code={code} {snippet[:100]}", usable


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--quick", action="store_true", help="first prefix only")
    parser.add_argument("--host", default=HOST)
    args = parser.parse_args()

    token = _token()
    headers = {"Token": token, "Content-Type": "application/json"}

    prefixes = PREFIXES[:1] if args.quick else PREFIXES
    working: list[tuple[str, str]] = []

    for prefix in prefixes:
        base = f"{args.host}{prefix}".rstrip("/")
        print(f"\n=== base {base or args.host} ===")
        for path in PATHS:
            status, detail, usable = _probe(f"{base}{path}", headers)
            flag = "  <== WORKS" if usable else ""
            print(f"  {path:42} HTTP {status}  {detail}{flag}")
            if usable:
                working.append((base, path))

    print("\n" + "=" * 70)
    if working:
        print("Working combinations:")
        for base, path in working:
            print(f"  base={base}\n  path={path}")
        print("\nPin GHN_BASE_URL to the base above, then map the path into ghn_client.py.")
        return 0

    print("Nothing worked. Two likely causes:")
    print("  1. The token is not valid for this API generation (401/403 would show it)")
    print("     - but every response is a routing 404, so the host/path is the problem.")
    print("  2. This gateway needs a partner id in the path, e.g. /shiip/public-api/{partner_id}/...")
    print("\nNext: fetch the GHN developer docs for the exact URL template, or check")
    print("whether the token in .env is a partner token with a prefix.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
