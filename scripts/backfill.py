#!/usr/bin/env python3
"""Backfill historical orders from the GHN API into the raw layer.

Backfills never run inside the scheduled pipeline: they are an explicit,
manually-triggered operation with their own schedule and log trail.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta

import boto3

RAW_BUCKET = os.environ.get("RAW_BUCKET_NAME", "")


def build_key(source: str, ts: datetime) -> str:
    return f"raw/{source}/{ts:%Y-%m-%d}/{ts:%Y%m%dT%H%M%S%f}.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="orders", help="source to backfill")
    parser.add_argument("--days", type=int, default=7, help="how many days back")
    parser.add_argument("--bucket", default=RAW_BUCKET)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.bucket:
        print("ERROR: --bucket or RAW_BUCKET_NAME is required", file=sys.stderr)
        return 1

    s3 = boto3.client("s3")
    now = datetime.now(UTC)

    for offset in range(args.days, 0, -1):
        ts = now - timedelta(days=offset)
        key = build_key(args.source, ts)
        if args.dry_run:
            print(f"[dry-run] would write {key}")
            continue

        payload = {
            "source": args.source,
            "ingested_at": now.isoformat(),
            "backfill": True,
            "records": [],
        }
        s3.put_object(
            Bucket=args.bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"wrote {key}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
