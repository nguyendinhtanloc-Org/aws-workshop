"""Build and deploy the SQS Processor to LocalStack, including Bronze configuration."""
from __future__ import annotations

import io
import json
import os
import platform
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from build_lambdas import build  # noqa: E402

ROOT = SCRIPTS_DIR.parent
LAMBDA_RUNTIME_IMAGE = "public.ecr.aws/lambda/python:3.12"
DOCKER_PLATFORMS = {
    "arm64": "linux/arm64",
    "x86_64": "linux/amd64",
}


def normalize_lambda_arch(raw: str) -> str:
    """Normalize a Docker/OS CPU architecture name to AWS Lambda's arm64 or x86_64."""
    value = raw.strip().lower()
    if value in {"arm64", "aarch64", "arm64v8", "arm64/v8"}:
        return "arm64"
    if value in {"x86_64", "amd64", "x64"}:
        return "x86_64"
    raise ValueError(f"Unsupported architecture {raw!r}; expected arm64 or x86_64")


def detect_local_lambda_arch() -> str:
    """Match LocalStack's Lambda container arch to the Docker daemon (or host OS)."""
    override = os.environ.get("LOCAL_LAMBDA_ARCH", "").strip()
    if override:
        return normalize_lambda_arch(override)

    docker = shutil.which("docker")
    if docker:
        try:
            proc = subprocess.run(
                [docker, "info", "--format", "{{.Architecture}}"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                return normalize_lambda_arch(proc.stdout.strip())
        except (OSError, subprocess.TimeoutExpired, ValueError):
            pass

    return normalize_lambda_arch(platform.machine())


def ensure_lambda_runtime_image(arch: str, image: str = LAMBDA_RUNTIME_IMAGE) -> None:
    """Ensure the cached LocalStack Python 3.12 Lambda image matches the host arch.

    Docker Desktop (containerd store) can keep a stale wrong-arch manifest under the
    same tag (e.g. arm64 cached on an x86_64 host), causing exec format error and
    startup timeouts inside LocalStack. Probing python3 catches both missing images
    and arch mismatches across Windows, macOS, and Linux.
    """
    docker = shutil.which("docker")
    if not docker:
        return

    try:
        probe = subprocess.run(
            [
                docker,
                "run",
                "--rm",
                "--pull=never",
                "--entrypoint",
                "python3",
                image,
                "-c",
                "import platform; print(platform.machine())",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if probe.returncode == 0 and probe.stdout.strip():
            cached_arch = normalize_lambda_arch(probe.stdout.strip().splitlines()[-1])
            if cached_arch == arch:
                return
    except (OSError, subprocess.TimeoutExpired, ValueError):
        pass

    target_platform = DOCKER_PLATFORMS[arch]
    print(f"Ensuring {image} for {target_platform}...")
    subprocess.run([docker, "rmi", "-f", image], capture_output=True, text=True, check=False)
    subprocess.run([docker, "pull", "--platform", target_platform, image], check=True)


def zip_directory(package: Path) -> bytes:
    """Build a zip archive with explicit POSIX permissions for Linux Lambda containers."""
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(package.rglob("*")):
            if path.is_file():
                info = zipfile.ZipInfo(path.relative_to(package).as_posix())
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                bundle.writestr(info, path.read_bytes())
    return archive.getvalue()


def main() -> None:
    load_dotenv(ROOT / ".env")
    endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    if urlsplit(endpoint).hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("LOCALSTACK_ENDPOINT must use loopback")
    # Reuse the initialization clients, whose credentials target only the local emulator.
    from init_local_env import _get_client

    arch = detect_local_lambda_arch()
    ensure_lambda_runtime_image(arch)

    sys.path.insert(0, str(ROOT / "src"))
    sys.path.insert(0, str(ROOT / "src/lambda/processor"))
    from bronze import prepare_table
    prepare_table(os.environ.get("MINIO_BRONZE_BUCKET", "ghn-analytics-bronze"))

    # Keep the local arch-specific build separate from build/lambda-processor (CDK ARM64).
    package = build(f"lambda-processor-local-{arch}", ROOT / "src/lambda/processor", arch=arch)
    zip_bytes = zip_directory(package)

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
        client.create_function(FunctionName=name, Code={"ZipFile": zip_bytes}, Architectures=[arch], **config)
        client.get_waiter("function_active_v2").wait(FunctionName=name)
    else:
        client.update_function_code(FunctionName=name, ZipFile=zip_bytes, Architectures=[arch])
        client.get_waiter("function_updated_v2").wait(FunctionName=name)
        client.update_function_configuration(FunctionName=name, **config)
        client.get_waiter("function_updated_v2").wait(FunctionName=name)
    mappings = client.list_event_source_mappings(FunctionName=name, EventSourceArn=queue_arn)["EventSourceMappings"]
    if mappings:
        client.update_event_source_mapping(UUID=mappings[0]["UUID"], Enabled=True, BatchSize=1)
    else:
        client.create_event_source_mapping(FunctionName=name, EventSourceArn=queue_arn, Enabled=True, BatchSize=1)
    print(f"Deployed {name} ({arch}): SQS FIFO -> DynamoDB + MinIO Bronze (Iceberg via Trino)")


if __name__ == "__main__":
    main()
