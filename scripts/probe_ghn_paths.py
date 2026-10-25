#!/usr/bin/env python3
"""Find which GHN endpoint paths work with the current base URL.

The GHN API moved to https://online-gateway.ghn.vn/shiip/public-api and
has changed endpoint paths between generations. This script probes the
candidate paths read-only and reports which one answers with data, so
ghn_client.py can be pinned to the working set.

Usage:
    python3 scripts/probe_ghn_paths.py            # probes everything
    python3 scripts/probe_ghn_paths.py -v         # show sample payload
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


def _token() -> str:
    token = os.environ.get("GHN_API_TOKEN")
    if not token:
        sys.exit("ERROR: set GHN_API_TOKEN (or put it in .env)")
    return token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base",
        default=os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn/shiip/public-api"),
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="show the first record of any 200 response")
    args = parser.parse_args()

    token = _token()
    headers = {"Token": token, "Content-Type": "application/json"}

    # (label, method, path, body). The GHN public API is GET-heavy, but a
    # few endpoints (order fetch, transport cost) are POST.
    CANDIDATES = [
        ("getProvinces", "GET", "/api/v1/apiv3/getProvinces", None),
        ("getProvinces", "GET", "/api/v1/apiv4/getProvinces", None),
        ("getDistricts", "GET", "/api/v1/apiv3/getDistricts", None),
        ("getDistricts", "GET", "/api/v1/apiv4/getDistricts", None),
        ("getWard", "GET", "/api/v1/apiv3/getWard", None),
        ("getWardsByDistrict", "GET", "/api/v1/apiv4/getWardsByDistrict", {"district_id": 268}),
        ("getOrder", "GET", "/api/v1/apiv3/getOrder", None),
        ("getOrderList", "POST", "/api/v1/apiv4/getOrderList", {"data": {}, "time": 1690000000000, "page_index": 1}),
        ("getTransportCost", "POST", "/api/v1/apiv4/getTransportCost", {"data": {"from_district_id": 268, "to_district_id": 760, "service_code": "NVX"}}),
    ]

    print(f"base = {args.base}\n")
    working: list[str] = []
    for label, method, path, body in CANDIDATES:
        url = f"{args.base}{path}"
        try:
            if method == "GET":
                r = requests.get(url, headers=headers, params=body, timeout=10)
            else:
                r = requests.post(url, headers=headers, json=body, timeout=10)
        except requests.RequestException as exc:
            print(f"  {label:16} {method:3} {path}  UNREACHABLE ({type(exc).__name__})")
            continue

        detail = ""
        ok = False
        if r.status_code == 200:
            try:
                payload = r.json()
                code = payload.get("code")
                ok = code in (200, None)
                detail = f"code={code} rows={len(payload.get('data') or [])}"
                if ok and args.verbose:
                    data = payload.get("data") or []
                    detail += " first=" + json.dumps(data[0] if isinstance(data, list) and data else (data if data else {}), ensure_ascii=False)[:200]
            except ValueError:
                detail = f"non-JSON ({r.headers.get('Content-Type', '?')})"
        else:
            detail = r.text[:80].replace("\n", " ")

        print(f"  {label:16} {method:3} {path}  HTTP {r.status_code}  {detail}")
        if ok:
            working.append(f"{label}: {method} {path}")

    print()
    if working:
        print("Working endpoints:")
        for w in working:
            print("  " + w)
    else:
        print("No endpoint returned usable data. If every call is 401/403, the token is invalid or belongs to a different API generation.")
    return 0 if working else 1


if __name__ == "__main__":
    sys.exit(main())
