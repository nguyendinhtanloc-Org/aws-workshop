"""Configuration loader for the Ingestor.

Supports two modes so the same code runs locally and in AWS:

- **Local** (no `GHN_SECRET_ARN` set): the token comes from the
  `GHN_API_TOKEN` environment variable, which `scripts/` and `.env` set.
- **AWS** (`GHN_SECRET_ARN` set): the token comes from Secrets Manager.

Configuration is resolved lazily via :func:`get_config` rather than at
import time, so importing this module never performs an AWS call. That
keeps unit tests and local scripts free of network dependencies.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

try:  # pragma: no cover - depends on local environment
    from dotenv import load_dotenv

    load_dotenv()  # no-op if no .env file is present
except ImportError:  # pragma: no cover - dotenv absent in Lambda runtime
    pass


def _from_secrets_manager(secret_arn: str) -> str:
    """Read the GHN API token from AWS Secrets Manager."""
    import boto3

    resp = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    return json.loads(resp["SecretString"])["token"]


def _from_environment() -> str:
    """Read the GHN API token from the environment (local development)."""
    token = os.environ.get("GHN_API_TOKEN")
    if not token:
        raise RuntimeError(
            "No GHN API token found. Set GHN_API_TOKEN in your environment "
            "(or in a .env file), or set GHN_SECRET_ARN to read it from "
            "AWS Secrets Manager."
        )
    return token


def get_token() -> str:
    """Return the GHN API token from Secrets Manager or the environment."""
    secret_arn = os.environ.get("GHN_SECRET_ARN")
    if secret_arn:
        return _from_secrets_manager(secret_arn)
    return _from_environment()


@lru_cache(maxsize=1)
def get_config() -> dict[str, Any]:
    """Return the configuration the Ingestor needs.

    Cached because it may perform a Secrets Manager call. Call
    ``get_config.cache_clear()`` if the environment changes mid-process.
    """
    return {
        "ghn_token": get_token(),
        "raw_bucket": os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"),
        "kinesis_stream": os.environ.get("KINESIS_STREAM_NAME", ""),
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
    }
