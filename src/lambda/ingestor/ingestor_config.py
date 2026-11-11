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

import contextlib
import json
import os
from functools import lru_cache
from typing import Any

with contextlib.suppress(ImportError):  # pragma: no cover - dotenv absent in Lambda runtime
    from dotenv import load_dotenv

    load_dotenv()  # no-op if no .env file is present


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
    """Return the configuration the Ingestor needs.

    Cached because it may perform a Secrets Manager call. Call
    ``get_config.cache_clear()`` if the environment changes mid-process.

    The gateway is the bare GHN host (not the /shiip/public-api docs
    portal). ``GHN_SHOP_ID`` is the second auth header the current
    gateway generation requires; it is optional in the mock and in
    generation v3, where only the token was sent.
    """
    return {
        "ghn_token": get_token(),
        "ghn_base_url": os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn"),
        "ghn_shop_id": os.environ.get("GHN_SHOP_ID", ""),
        # The API has no list-orders route, so the Ingestor polls a
        # seed watchlist of order codes (comma separated). For a live
        # shop this watchlist grows as orders are created; status
        # *changes* themselves arrive by webhook (docs/runbook.md).
        "ghn_order_codes": os.environ.get("GHN_ORDER_CODES", ""),
        # The ward endpoint only answers per district, so fetch wards
        # for this bounded list of district ids only (comma separated).
        "ghn_ward_district_ids": os.environ.get("GHN_WARD_DISTRICTS", ""),
        "raw_bucket": os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"),
        "kinesis_stream": os.environ.get("KINESIS_STREAM_NAME", ""),
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
    }
