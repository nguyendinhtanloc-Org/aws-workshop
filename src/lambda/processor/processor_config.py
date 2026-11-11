"""Configuration loader for the Processor.

The Processor reads its settings from environment variables in both local
and AWS runs - the only difference is the values, not the mechanism.
"""
from __future__ import annotations

import contextlib
import os
from typing import Any

with contextlib.suppress(ImportError):  # pragma: no cover - dotenv absent in Lambda runtime
    from dotenv import load_dotenv

    load_dotenv()  # no-op if no .env file is present


def get_config() -> dict[str, Any]:
    """Return the configuration the Processor needs."""
    return {
        "orders_table": os.environ.get("ORDERS_TABLE_NAME", "ghn-analytics-dev-orders"),
        "raw_bucket": os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"),
        "curated_bucket": os.environ.get("CURATED_BUCKET_NAME", "ghn-analytics-dev-curated"),
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
    }
