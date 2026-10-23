#!/usr/bin/env python3
"""Replay failed messages from an SQS DLQ back into the pipeline.

Usage:
    python scripts/replay_dlq.py <dlq-url> [--limit 100] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import sys

import boto3


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dlq_url")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sqs = boto3.client("sqs")
    queue = sqs.get_queue_attributes(QueueUrl=args.dlq_url, AttributeNames=["ApproximateNumberOfMessages"])
    depth = int(queue["Attributes"].get("ApproximateNumberOfMessages", 0))
    print(f"DLQ depth: {depth} message(s)")

    if depth == 0:
        print("Nothing to replay.")
        return 0

    replayed = 0
    while replayed < args.limit:
        received = sqs.receive_message(
            QueueUrl=args.dlq_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=30,
            WaitTimeSeconds=2,
        )
        messages = received.get("Messages", [])
        if not messages:
            break

        for message in messages:
            body = message.get("Body", "")
            try:
                parsed = json.loads(body)
                print(f"  {message['MessageId']}: {json.dumps(parsed)[:120]}")
            except json.JSONDecodeError:
                print(f"  {message['MessageId']}: <non-json body>")

            if not args.dry_run:
                sqs.delete_message(QueueUrl=args.dlq_url, ReceiptHandle=message["ReceiptHandle"])
            replayed += 1

        if args.dry_run:
            break

    print(f"{'would replay' if args.dry_run else 'replayed'} {replayed} message(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
