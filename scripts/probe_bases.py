#!/usr/bin/env python3
"""Probe candidate GHN API base URLs. Read-only, no AWS needed."""
from __future__ import annotations

import json
import os
import sys

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

token = os.environ.get("GHN_API_TOKEN")
if not token:
    sys.exit("ERROR: set GHN_API_TOKEN (or put it in .env)")

BASES = [
    "https://shippining.1clickdev.com",  # GHN API v4
    "https://shippining.xyz",            # GHN API v3
    "https://developer-doc.ghn.vn",
    "https://api.ghn.vn",
]

PATHS = {
    "getProvinces": "/api/v1/apiv3/getProvinces",
    "getOrder": "/api/v1/apiv3/getOrder",
}

print(f"token={token[:6]}... probing {len(BASES)} base URL(s)\n")

for base in BASES:
    print(f"--- {base} ---")
    for name, path in PATHS.items():
        try:
            r = requests.get(
                f"{base}{path}",
                headers={"Token": token, "Content-Type": "application/json"},
                timeout=10,
            )
        except requests.RequestException as exc:
            print(f"  {name:14} UNREACHABLE ({type(exc).__name__})")
            continue
        info = ""
        if r.status_code == 200:
            try:
                payload = r.json()
                data = payload.get("data")
                if isinstance(data, list):
                    info = f"code={payload.get('code')} rows={len(data)}"
                else:
                    info = f"code={payload.get('code')}"
            except ValueError:
                info = f"content-type={r.headers.get('Content-Type','?')}"
        else:
            try:
                info = f"code={r.json().get('code')!r}"
            except ValueError:
                info = r.text[:60].replace("\n", " ")
        print(f"  {name:14} HTTP {r.status_code} {info}")
