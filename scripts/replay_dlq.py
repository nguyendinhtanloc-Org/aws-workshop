#!/usr/bin/env python3
"""Replay failed records from a DLQ back into the pipeline.

Usage:
    python3 scripts/replay_dlq.py <dlq-url> [--limit 100] [--dry-run]

A DLQ message holds a rejected record plus the source that produced it.
Replaying means re-publishing the record to the S3 raw layer under its
original prefix, which re-triggers the Processor via the S3 notification.
The write is idempotent (the Processor uses a conditional put), so a
replay can never create a duplicate row.

The message is deleted from the DLQ only after the re-publish succeeds.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import boto3


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dlq_url")
    parser.add_argument(
        "--bucket",
        default=os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"),
        help="raw bucket to re-publish into",
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sqs = boto3.client("sqs")
    s3 = boto3.client("s3")

    queue = sqs.get_queue_attributes(
        QueueUrl=args.dlq_url, AttributeNames=["ApproximateNumberOfMessages"]
    )
    depth = int(queue["Attributes"].get("ApproximateNumberOfMessages", 0))
    print(f"DLQ depth: {depth} message(s)")
    if depth == 0:
        return 0

    replayed = 0
    failed = 0
    while replayed + failed < args.limit:
        received = sqs.receive_message(
            QueueUrl=args.dlq_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=60,
            WaitTimeSeconds=2,
        )
        messages = received.get("Messages", [])
        if not messages:
            break

        for message in messages:
            try:
                body = json.loads(message["Body"])
            except json.JSONDecodeError:
                print(f"  ! non-JSON body, keeping for manual review: {message['MessageId']}")
                break

            source = body.get("source", "unknown")
            order = body.get("order")
            reason = body.get("reason")

            if args.dry_run:
                print(f"  [dry-run] {message['MessageId']} source={source} reason={reason}")
                print(f"            {json.dumps(order, ensure_ascii=False)[:160]}")
                replayed += 1
                continue

            key = f"{source}/replay/{message['MessageId']}.json"
            s3.put_object(
                Bucket=args.bucket,
                Key=key,
                Body=json.dumps({"records": [order]}, ensure_ascii=False).encode("utf-8"),
                ContentType="application/json",
            )
            print(f"  replayed {message['MessageId']} -> s3://{args.bucket}/{key}")
            sqs.delete_message(QueueUrl=args.dlq_url, ReceiptHandle=message["ReceiptHandle"])
            replayed += 1

    print(f"{'would replay' if args.dry_run else 'replayed'} {replayed} message(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
