#!/usr/bin/env python3
"""Print local S3 JSON, DynamoDB items and up to 10 available SQS messages.

Usage:
    python scripts/inspect_local_data.py

SQS messages are received and then made visible again, never deleted.
Receiving increments their receive count; use this against the local demo queue.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys

ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
REGION = "ap-southeast-1"

try:
    import boto3
except ImportError:
    print("boto3 is required: python -m pip install boto3")
    sys.exit(1)


def get_client(service: str):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def print_json_or_text(text: str) -> None:
    try:
        print(json.dumps(json.loads(text), ensure_ascii=False, indent=2))
    except json.JSONDecodeError:
        print(text)


def inspect_s3(order_code: str | None = None) -> None:
    print("\n1. S3 files and contents (LocalStack)")
    s3 = get_client("s3")
    try:
        name = os.environ.get("LOCAL_RAW_BUCKET_NAME", "ghn-analytics-local-raw")
        print(f"\nBucket: {name}")
        found = False
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name):
            for obj in page.get("Contents", []):
                if order_code and order_code not in obj["Key"]:
                    continue
                found = True
                key = obj["Key"]
                print(f"\nFile: s3://{name}/{key} ({obj['Size']} bytes)")
                try:
                    response = s3.get_object(Bucket=name, Key=key)
                    with response["Body"] as stream:
                        body = stream.read()
                    if body.startswith(b"\x1f\x8b"):
                        body = gzip.decompress(body)
                    if body[:4] == b"PAR1":
                        print("Parquet binary file; JSON content unavailable.")
                        continue
                    print_json_or_text(body.decode("utf-8"))
                except UnicodeDecodeError:
                    print("Binary file; JSON content unavailable.")
                except Exception as exc:
                    print(f"Cannot read file: {exc}")
        if not found:
            print("(empty)")
    except Exception as exc:
        print(f"S3 inspection failed: {exc}")


def inspect_dynamodb(order_code: str | None = None) -> None:
    print("\n2. DynamoDB items (LocalStack)")
    ddb = boto3.resource(
        "dynamodb",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    for name in ("ghn-analytics-local-orders", "ghn-analytics-local-watchlist"):
        print(f"\nTable: {name}")
        try:
            table = ddb.Table(name)
            kwargs = {}
            count = 0
            while True:
                response = table.scan(**kwargs)
                for item in response.get("Items", []):
                    if order_code and order_code not in (str(item.get("sk", "")), str(item.get("order_code", ""))):
                        continue
                    count += 1
                    print(json.dumps(item, ensure_ascii=False, indent=2, default=str))
                if not response.get("LastEvaluatedKey"):
                    break
                kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            print(f"Total: {count} items")
        except Exception as exc:
            print(f"DynamoDB inspection failed: {exc}")


def inspect_sqs(order_code: str | None = None) -> None:
    print("\n3. SQS messages (LocalStack)")
    sqs = get_client("sqs")
    queue_name = os.environ.get("LOCAL_WEBHOOK_QUEUE_NAME", "ghn-analytics-local-webhook.fifo")
    try:
        queue_url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )["Attributes"]
        print(f"Queue: {queue_name}")
        print(f"Available (approximate): {attrs.get('ApproximateNumberOfMessages', '0')}")
        print(f"In flight (approximate): {attrs.get('ApproximateNumberOfMessagesNotVisible', '0')}")
        messages = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            VisibilityTimeout=30,
            MessageSystemAttributeNames=["All"],
            MessageAttributeNames=["All"],
        ).get("Messages", [])
        try:
            for message in messages:
                if order_code and order_code not in message["Body"]:
                    continue
                print(f"\nMessageId: {message['MessageId']}")
                print("Attributes:")
                print(json.dumps(message.get("Attributes", {}), ensure_ascii=False, indent=2))
                if message.get("MessageAttributes"):
                    print("Message attributes:")
                    print(json.dumps(message["MessageAttributes"], ensure_ascii=False, indent=2, default=str))
                print("Body:")
                print_json_or_text(message["Body"])
            if not messages:
                print("No message returned in this read.")
            print("This read shows up to 10 available messages; FIFO may return fewer.")
        finally:
            for message in messages:
                try:
                    sqs.change_message_visibility(
                        QueueUrl=queue_url,
                        ReceiptHandle=message["ReceiptHandle"],
                        VisibilityTimeout=0,
                    )
                except Exception as exc:
                    print(f"Cannot restore visibility for {message['MessageId']}: {exc}")
        print("Messages were not deleted; receiving increases their receive count.")
    except Exception as exc:
        print(f"SQS inspection failed: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-code", help="show objects/items/messages for one order")
    args = parser.parse_args()
    print("Inspecting data in Docker LocalStack")
    inspect_s3(args.order_code)
    inspect_dynamodb(args.order_code)
    inspect_sqs(args.order_code)
    print("\nInspection complete.")


if __name__ == "__main__":
    main()
