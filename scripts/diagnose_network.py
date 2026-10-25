#!/usr/bin/env python3
"""Diagnose why the GHN API hosts are unreachable. No AWS needed.

Run from your venv so requests is available:
    python3 scripts/diagnose_network.py

It checks, for each candidate host: proxy env vars, DNS resolution, and a
raw TCP connection to port 443. Read-only, makes no API calls.
"""
from __future__ import annotations

import os
import socket
import ssl
import urllib.parse

HOSTS = [
    "shippining.1clickdev.com",
    "shippining.xyz",
    "api.ghn.vn",
    "developer.ghn.vn",
]

print("=== proxy environment ===")
for var in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "no_proxy", "all_proxy"):
    if os.environ.get(var):
        print(f"  {var} = {os.environ.get(var)}")
print("(none set)" if not any(os.environ.get(v) for v in ("http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY","all_proxy")) else "")

print("\n=== DNS + TCP 443 ===")
for host in HOSTS:
    # DNS
    try:
        addrs = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        ip = addrs[0][4][0]
    except Exception as exc:
        print(f"  {host:28} DNS FAIL ({exc})")
        continue

    # raw TCP
    try:
        s = socket.create_connection((ip, 443), timeout=6)
        s.close()
        print(f"  {host:28} dns={ip:18} tcp443=OK")
    except Exception as exc:
        print(f"  {host:28} dns={ip:18} tcp443=FAIL ({exc})")

print("\nNext step: the first host with tcp443=OK is your reachable GHN API base.")
