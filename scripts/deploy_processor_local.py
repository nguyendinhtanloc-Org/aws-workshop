"""Build and deploy the SQS Processor to LocalStack, including Bronze configuration."""
from __future__ import annotations

import io
import json
import os
import sys
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

from build_lambdas import build
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    load_dotenv(ROOT / ".env")
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    if urlsplit(endpoint).hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("LOCALSTACK_ENDPOINT must use loopback")
    # Reuse the initialization clients, whose credentials target only the local emulator.
    from init_local_env import _get_client

    sys.path.insert(0, str(ROOT / "src"))
    sys.path.insert(0, str(ROOT / "src/lambda/processor"))
    from bronze import prepare_table
    prepare_table(os.environ.get("MINIO_BRONZE_BUCKET", "ghn-analytics-bronze"))

    package = build("lambda-processor", ROOT / "src/lambda/processor")
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in package.rglob("*"):
            if path.is_file():
                bundle.write(path, path.relative_to(package).as_posix())

    iam = _get_client("iam")
    role_name = "ghn-local-processor"
    try:
        role = iam.get_role(RoleName=role_name)["Role"]
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }))["Role"]
    for policy in ("AWSLambdaSQSQueueExecutionRole", "AWSLambdaBasicExecutionRole"):
        iam.attach_role_policy(RoleName=role_name, PolicyArn=f"arn:aws:iam::aws:policy/service-role/{policy}")
    iam.put_role_policy(RoleName=role_name, PolicyName="LocalOrders", PolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow",
        "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DescribeTable"],
        "Resource": "arn:aws:dynamodb:ap-southeast-1:000000000000:table/ghn-analytics-local-orders"}],
    }))
    sqs = _get_client("sqs")
    queue_url = sqs.get_queue_url(QueueName="ghn-analytics-local-webhook.fifo")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    dlq = sqs.create_queue(QueueName="ghn-analytics-local-processor-dlq.fifo", Attributes={"FifoQueue": "true"})
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq["QueueUrl"], AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={
        "VisibilityTimeout": "720", "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "5"}),
    })
    client = _get_client("lambda")
    name = "ghn-analytics-local-processor"
    config = {
        "Role": role["Arn"],
        "Runtime": "python3.12",
        "Handler": "handler.lambda_handler",
        "Timeout": 120,
        "MemorySize": 1024,
        "Environment": {"Variables": {
            "ORDERS_TABLE_NAME": "ghn-analytics-local-orders",
            "AWS_ENDPOINT_URL": "http://localstack:4566",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", "admin"),
            "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", "password123"),
            "MINIO_BRONZE_BUCKET": os.environ.get("MINIO_BRONZE_BUCKET", "ghn-analytics-bronze"),
            "TRINO_ENDPOINT": "http://trino:8080",
            "BRONZE_ICEBERG_TABLE": os.environ.get("BRONZE_ICEBERG_TABLE", "ghn_events"),
        }},
    }
    try:
        client.get_function(FunctionName=name)
    except client.exceptions.ResourceNotFoundException:
        client.create_function(FunctionName=name, Code={"ZipFile": archive.getvalue()}, Architectures=["arm64"], **config)
        client.get_waiter("function_active_v2").wait(FunctionName=name)
    else:
        client.update_function_code(FunctionName=name, ZipFile=archive.getvalue(), Architectures=["arm64"])
        client.get_waiter("function_updated_v2").wait(FunctionName=name)
        client.update_function_configuration(FunctionName=name, **config)
        client.get_waiter("function_updated_v2").wait(FunctionName=name)
    mappings = client.list_event_source_mappings(FunctionName=name, EventSourceArn=queue_arn)["EventSourceMappings"]
    if mappings:
        client.update_event_source_mapping(UUID=mappings[0]["UUID"], Enabled=True, BatchSize=1)
    else:
        client.create_event_source_mapping(FunctionName=name, EventSourceArn=queue_arn, Enabled=True, BatchSize=1)
    print(f"Deployed {name}: SQS FIFO -> DynamoDB + MinIO Bronze (Iceberg via Trino)")


if __name__ == "__main__":
    main()
