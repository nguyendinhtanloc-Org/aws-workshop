"""Initialize local AWS resources on LocalStack and MinIO for testing."""
from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    import boto3
except ImportError:
    print("boto3 is required: pip install boto3")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    pass
else:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")

LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
REGION = "ap-southeast-1"


def _get_client(service: str, endpoint: str = LOCALSTACK_ENDPOINT):
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def init_s3():
    print("--> Initializing S3 buckets on LocalStack...")
    s3 = _get_client("s3")
    buckets = ["ghn-analytics-local-raw"]
    for b in buckets:
        try:
            s3.create_bucket(
                Bucket=b,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
            print(f"  [+] Created bucket {b}")
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                print(f"  [*] Bucket {b} already exists")
            else:
                print(f"  [-] Failed to create {b}: {e}")

    print("--> Initializing MinIO warehouse bucket...")
    s3_minio = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "password123"),
    )
    from botocore.exceptions import ClientError

    for bucket in dict.fromkeys(("warehouse", os.environ.get("MINIO_BRONZE_BUCKET", "ghn-analytics-bronze"))):
        try:
            s3_minio.head_bucket(Bucket=bucket)
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            s3_minio.create_bucket(Bucket=bucket)
        print(f"  [+] MinIO bucket '{bucket}' is ready")


def init_dynamodb():
    print("--> Initializing DynamoDB tables on LocalStack...")
    ddb = _get_client("dynamodb")
    tables = [
        ("ghn-analytics-local-orders", "pk", "sk"),
        ("ghn-analytics-local-watchlist", "pk", "sk"),
        ("ghn-analytics-local-state", "pk", "sk"),
    ]
    for table_name, pk, sk in tables:
        try:
            ddb.create_table(
                TableName=table_name,
                KeySchema=[
                    {"AttributeName": pk, "KeyType": "HASH"},
                    {"AttributeName": sk, "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": pk, "AttributeType": "S"},
                    {"AttributeName": sk, "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            print(f"  [+] Created DynamoDB table {table_name}")
        except Exception as e:
            if "ResourceInUseException" in str(e):
                print(f"  [*] Table {table_name} already exists")
            else:
                print(f"  [-] Failed to create {table_name}: {e}")


def init_sqs():
    print("--> Initializing SQS Queues on LocalStack...")
    sqs = _get_client("sqs")
    dlq_name = "ghn-analytics-local-dlq"
    try:
        resp = sqs.create_queue(QueueName=dlq_name)
        print(f"  [+] Created SQS DLQ {dlq_name}: {resp.get('QueueUrl')}")
    except Exception as e:
        print(f"  [-] Failed to create queue {dlq_name}: {e}")

    fifo_name = "ghn-analytics-local-webhook.fifo"
    try:
        resp = sqs.create_queue(
            QueueName=fifo_name,
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )
        print(f"  [+] Created SQS FIFO Queue {fifo_name}: {resp.get('QueueUrl')}")
    except Exception as e:
        print(f"  [-] Failed to create FIFO queue {fifo_name}: {e}")


def verify_webhook_resources():
    """Make the command fail when required LocalStack resources are absent."""
    s3 = _get_client("s3")
    ddb = _get_client("dynamodb")
    sqs = _get_client("sqs")
    s3.head_bucket(Bucket="ghn-analytics-local-raw")
    ddb.describe_table(TableName="ghn-analytics-local-watchlist")
    sqs.get_queue_url(QueueName="ghn-analytics-local-webhook.fifo")
    print("  [+] Webhook resources verified on LocalStack")


def main():
    print("=== GHN Logistics: Local Test Harness Initialization ===")
    try:
        init_s3()
        init_dynamodb()
        init_sqs()
        verify_webhook_resources()
        print("\nAll local resources initialized successfully!")
    except Exception as exc:
        print(f"\nInitialization failed (ensure Docker containers are up): {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()

