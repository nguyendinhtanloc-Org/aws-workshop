"""Configuration loader for the Ingestor."""
from __future__ import annotations

import contextlib
import json
import os
from functools import lru_cache
from typing import Any

from ghn_client import GhnClient

with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv()


def _from_secrets_manager(secret_arn: str) -> str:
    """Read the GHN API token from AWS Secrets Manager."""
    import boto3

    resp = boto3.client("secretsmanager").get_secret_value(SecretId=secret_arn)
    return str(json.loads(resp["SecretString"])["token"])


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
    """Return the configuration the Ingestor needs."""
    return {
        "ghn_token": get_token(),
        "ghn_base_url": os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn"),
        "ghn_shop_id": os.environ.get("GHN_SHOP_ID", ""),
        "ghn_ward_district_ids": os.environ.get("GHN_WARD_DISTRICTS", ""),
        "raw_bucket": os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"),
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
    }


def make_client() -> Any:
    """Build a GHN client for master data or reconciliation."""
    if os.environ.get("GHN_MOCK", "").lower() in ("1", "true", "yes"):
        from mock_source import MockGhnClient

        return MockGhnClient()

    config = get_config()
    return GhnClient(
        token=config["ghn_token"],
        base_url=config["ghn_base_url"],
        shop_id=config["ghn_shop_id"],
    )
