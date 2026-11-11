#!/usr/bin/env python3
"""Backfill historical data into the raw layer.

A backfill re-fetches a window of past days from the GHN API and lands it
in the raw bucket under the same key layout the scheduled Ingestor uses,
so the normal pipeline picks it up without special handling.

Backfills are explicit, manually-triggered operations with their own log
trail - they never run inside the scheduled pipeline.

Usage:
    export GHN_API_TOKEN=...
    python3 scripts/backfill.py --days 7 [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta

import boto3

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "lambda", "ingestor"))

from ghn_client import GhnClient, GhnClientError


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument(
        "--bucket", default=os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw")
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bucket", default=os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"))
    parser.add_argument(
        "--codes", default="",
        help="comma-separated order codes to backfill (default: GHN_ORDER_CODES)",
    )
    args = parser.parse_args()

    try:
        token = os.environ["GHN_API_TOKEN"]
    except KeyError:
        print("ERROR: set GHN_API_TOKEN (or .env with it)", file=sys.stderr)
        return 1

    # The GHN API has no list-orders route, so a backfill can only pull
    # the orders named in the watchlist (GHN_ORDER_CODES env var or the
    # --codes flag). Without codes there is nothing to backfill - say so
    # and stop instead of silently writing empty days.
    codes_arg = getattr(args, "codes", "") or os.environ.get("GHN_ORDER_CODES", "")
    order_codes = [c.strip() for c in codes_arg.split(",") if c.strip()]
    if not order_codes:
        print("No order codes to backfill: set GHN_ORDER_CODES (comma separated) "
              "or pass --codes. The GHN API cannot list orders.", file=sys.stderr)
        return 1

    client = GhnClient(token=token)
    s3 = boto3.client("s3")
    now = datetime.now(UTC)

    for offset in range(args.days, 0, -1):
        ts = now - timedelta(days=offset)
        day = ts.strftime("%Y-%m-%d")
        key = f"raw/backfill/orders/{day}.json"

        if args.dry_run:
            print(f"[dry-run] would write {key} for {len(order_codes)} order code(s)")
            continue

        try:
            records = client.fetch_orders(order_codes=order_codes)
        except GhnClientError as exc:
            print(f"ERROR: cannot fetch {day}: {exc}", file=sys.stderr)
            return 1

        body = json.dumps(
            {"source": "orders", "ingested_at": now.isoformat(), "backfill": True, "records": records},
            ensure_ascii=False,
            default=str,
        )
        s3.put_object(
            Bucket=args.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        print(f"wrote s3://{args.bucket}/{key} ({len(records)} records)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
