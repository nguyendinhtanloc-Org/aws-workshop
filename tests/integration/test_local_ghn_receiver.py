"""HTTP -> webhook handler -> real LocalStack. GHN detail is stubbed only."""
from __future__ import annotations

import json
import os
import sys
import threading
import uuid
from importlib import import_module
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import boto3
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src" / "lambda" / "ingestor"))
import ghn_client  # noqa: E402
import serve_webhook_local as local  # noqa: E402
import webhook_handler  # noqa: E402

TEST_SECRET = "test-shared-secret-32-characters-long"


def _post(url: str, body: bytes, token: str = TEST_SECRET) -> int:
    request = Request(url, data=body, headers={"X-GHN-Webhook-Token": token, "Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            return response.status
    except HTTPError as exc:
        return exc.code


@pytest.fixture
def local_http(monkeypatch):
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    suffix = uuid.uuid4().hex[:10]
    bucket = f"ghn-staging-test-{suffix}"
    table_name = f"ghn-staging-test-{suffix}"
    queue_name = f"ghn-staging-test-{suffix}.fifo"
    region = "ap-southeast-1"
    s3 = boto3.client("s3", endpoint_url=endpoint, region_name=region, aws_access_key_id="test", aws_secret_access_key="test")
    ddb = boto3.client("dynamodb", endpoint_url=endpoint, region_name=region, aws_access_key_id="test", aws_secret_access_key="test")
    sqs = boto3.client("sqs", endpoint_url=endpoint, region_name=region, aws_access_key_id="test", aws_secret_access_key="test")
    try:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
        ddb.create_table(TableName=table_name, KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}], AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
        queue_url = sqs.create_queue(QueueName=queue_name, Attributes={"FifoQueue": "true"})["QueueUrl"]
    except Exception as exc:
        pytest.skip(f"LocalStack unavailable: {type(exc).__name__}")
    values = {
        "LOCALSTACK_ENDPOINT": endpoint,
        "LOCAL_RAW_BUCKET_NAME": bucket,
        "LOCAL_WATCHLIST_TABLE_NAME": table_name,
        "LOCAL_WEBHOOK_QUEUE_NAME": queue_name,
        "GHN_BASE_URL": "https://dev-online-gateway.ghn.vn",
        "GHN_API_TOKEN": "test-staging-token",
        "GHN_SHOP_ID": "123",
        "GHN_CALLBACK_SHARED_TOKEN": TEST_SECRET,
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(ghn_client.GhnClient, "fetch_orders", lambda self, order_codes: [{"order_code": order_codes[0], "created_date": "2026-11-16T08:00:00Z"}])
    previous_clients = webhook_handler._clients.copy()
    with patch.dict(os.environ):
        local.configure_localstack()
        server = local.WebhookServer(("127.0.0.1", 0), local.Receiver)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}", s3, ddb, sqs, bucket, table_name, queue_url
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            webhook_handler._clients.clear()
            webhook_handler._clients.update(previous_clients)


@pytest.fixture
def bronze_storage(monkeypatch):
    if os.environ.get("RUN_MINIO_INTEGRATION") != "1":
        yield None
        return

    from botocore.config import Config

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "password123")
    bucket = f"ghn-bronze-test-{uuid.uuid4().hex[:12]}"
    for name, value in (
        ("MINIO_ENDPOINT", endpoint), ("MINIO_ACCESS_KEY", access_key),
        ("MINIO_SECRET_KEY", secret_key), ("MINIO_BRONZE_BUCKET", bucket),
    ):
        monkeypatch.setenv(name, value)
    client = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id=access_key,
        aws_secret_access_key=secret_key, region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    client.create_bucket(Bucket=bucket)
    monkeypatch.syspath_prepend(str(ROOT / "src" / "lambda" / "processor"))
    from bronze import execute_sql

    iceberg_table = "test_" + uuid.uuid4().hex
    monkeypatch.setenv("BRONZE_ICEBERG_TABLE", iceberg_table)
    try:
        yield client, bucket
    finally:
        execute_sql(f"DROP TABLE IF EXISTS iceberg.bronze.{iceberg_table}")
        for obj in client.list_objects_v2(Bucket=bucket).get("Contents", []):
            client.delete_object(Bucket=bucket, Key=obj["Key"])
        client.delete_bucket(Bucket=bucket)


def test_real_http_localstack_duplicate_and_failures(local_http, monkeypatch, bronze_storage):
    base, s3, _ddb, sqs, bucket, table_name, queue_url = local_http
    code = "TEST-" + uuid.uuid4().hex[:10]
    # Shape follows GHN's published callback example; values here are test data.
    callback = {
        "ShopID": 123, "Time": "2026-11-17T04:49:47.811Z", "OrderCode": code,
        "ClientOrderCode": "CLIENT-1", "Type": "create", "Description": "Tao don hang",
        "Status": "ready_to_pick", "Reason": "", "ReasonCode": "", "CODAmount": 0,
        "CODTransferDate": None, "Weight": 600, "ConvertedWeight": 800,
        "Length": 25, "Width": 20, "Height": 8, "PaymentType": 2,
        "IsPartialReturn": False, "PartialReturnCode": "", "Fee": {},
        "TotalFee": 0, "Warehouse": "", "ShipperName": "", "ShipperPhone": "",
        "PodURL": "",
    }
    body = json.dumps(callback, separators=(",", ":")).encode()
    url = base + "/webhook/ghn"
    assert urlopen(base + "/health").status == 200
    oversized = Request(url, data=b"", headers={"Content-Length": str(local.MAX_BODY_BYTES + 1)})
    with pytest.raises(HTTPError) as rejected:
        urlopen(oversized, timeout=10)
    assert rejected.value.code == 413
    assert _post(url, body, "wrong") == 401
    assert _post(url, b"{broken") == 400
    assert _post(url, body) == 200
    item = boto3.resource("dynamodb", endpoint_url=os.environ["LOCALSTACK_ENDPOINT"], region_name="ap-southeast-1", aws_access_key_id="test", aws_secret_access_key="test").Table(table_name).get_item(Key={"pk": "#WATCH", "sk": code}, ConsistentRead=True)["Item"]
    assert item["terminal"] is False
    first_registered = item["registered_at"]
    objects = s3.list_objects_v2(Bucket=bucket)["Contents"]
    assert len(objects) == 1
    assert all(o["Key"].startswith("raw/webhook/") for o in objects)
    normalized = next(o["Key"] for o in objects if o["Key"].startswith("raw/webhook/"))
    record = json.loads(s3.get_object(Bucket=bucket, Key=normalized)["Body"].read())["records"][0]
    assert record["created_at"] == "2026-11-16T08:00:00Z"
    message = sqs.receive_message(
        QueueUrl=queue_url, WaitTimeSeconds=2, MessageAttributeNames=["All"]
    )["Messages"][0]
    assert json.loads(message["Body"])["order_code"] == code
    assert json.loads(message["Body"])["created_at"] == record["created_at"]

    monkeypatch.syspath_prepend(str(ROOT / "src" / "lambda" / "processor"))
    processor = import_module("src.lambda.processor.handler")
    orders = boto3.resource(
        "dynamodb",
        endpoint_url=os.environ["LOCALSTACK_ENDPOINT"],
        region_name="ap-southeast-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ).create_table(
        TableName=f"{table_name}-orders",
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    monkeypatch.setitem(processor._clients, "table", orders)
    event = {
        "Records": [{
            "eventSource": "aws:sqs",
            "messageId": message["MessageId"],
            "body": message["Body"],
            "messageAttributes": {
                key: {"stringValue": value["StringValue"]}
                for key, value in message["MessageAttributes"].items()
            },
        }],
    }
    assert processor.lambda_handler(event, None)["written"] == 1
    if bronze_storage:
        from bronze import execute_sql, prepare_table

        # Simulate a lost catalog entry while keeping all MinIO objects.
        execute_sql(
            "CALL iceberg.system.unregister_table(schema_name => 'bronze', "
            f"table_name => '{os.environ['BRONZE_ICEBERG_TABLE']}')"
        )
        prepare_table.cache_clear()
    assert processor.lambda_handler(event, None)["duplicates"] == 1
    stored = orders.get_item(
        Key={"pk": f"ORDER#{code}", "sk": "2026-11-16"}, ConsistentRead=True
    )["Item"]
    assert stored["order_code"] == code
    assert stored["status"] == "pending"
    if bronze_storage:
        from bronze import execute_sql
        from run_processor_local import consume_one

        minio, bronze_bucket = bronze_storage
        sqs.change_message_visibility(
            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"], VisibilityTimeout=0
        )
        assert consume_one(sqs, queue_url, processor.lambda_handler, wait=2)
        keys = [obj["Key"] for obj in minio.list_objects_v2(Bucket=bronze_bucket)["Contents"]]
        raw_keys = [key for key in keys if key.startswith("raw/")]
        assert len(raw_keys) == 1
        raw_key = raw_keys[0]
        assert raw_key.startswith("raw/2026-11-17_")
        assert raw_key.endswith(".json")
        raw = minio.get_object(Bucket=bronze_bucket, Key=raw_key)["Body"].read()
        assert json.loads(raw) == record
        rows = execute_sql(
            "SELECT raw_uri, source, order_code, CAST(event_date AS varchar) "
            f"FROM iceberg.bronze.{os.environ['BRONZE_ICEBERG_TABLE']}"
        )
        assert rows == [[f"s3://{bronze_bucket}/{raw_key}", "ghn_webhook", code, "2026-11-17"]]
        assert any(key.endswith(".metadata.json") for key in keys)
        assert any(key.endswith(".parquet") for key in keys)
    else:
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"])
    assert _post(url, body) == 200
    second = boto3.resource("dynamodb", endpoint_url=os.environ["LOCALSTACK_ENDPOINT"], region_name="ap-southeast-1", aws_access_key_id="test", aws_secret_access_key="test").Table(table_name).get_item(Key={"pk": "#WATCH", "sk": code}, ConsistentRead=True)["Item"]
    assert second["registered_at"] == first_registered
    switched = {**callback, "Type": "switch_status", "Status": "picking", "Time": "2026-11-17T05:49:47.811Z"}
    assert _post(url, json.dumps(switched).encode()) == 200
    status_message = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)["Messages"][0]
    assert json.loads(status_message["Body"])["status"] == "picking"
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=status_message["ReceiptHandle"])

    class FailingS3:
        def put_object(self, **kwargs):
            raise RuntimeError("injected S3 failure")

    class FailingSQS:
        def send_message(self, **kwargs):
            raise RuntimeError("injected SQS failure")

    real_s3, real_sqs = webhook_handler._clients["s3"], webhook_handler._clients["sqs"]
    webhook_handler._clients["s3"] = FailingS3()
    try:
        assert _post(url, body) == 500
    finally:
        webhook_handler._clients["s3"] = real_s3
    webhook_handler._clients["sqs"] = FailingSQS()
    try:
        assert _post(url, body) == 500
    finally:
        webhook_handler._clients["sqs"] = real_sqs
    assert _post(url, body) == 200
