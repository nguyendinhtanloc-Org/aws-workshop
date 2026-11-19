"""Refresh GHN provinces, districts and wards in the raw bucket."""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from ingestor_config import make_client
from validator import validate_master_data


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Fetch and archive master data."""
    sources = event.get("sources")
    if sources and "master_data" not in sources:
        return {"written": {}, "errors": {}}

    import boto3

    district_ids = [
        int(value) for value in os.environ.get("GHN_WARD_DISTRICTS", "").split(",")
        if value.strip()
    ]
    records = make_client().fetch_master_data(ward_district_ids=district_ids or None)
    validate_master_data(records)
    now = datetime.now(UTC)
    key = f"raw/master_data/{now:%Y-%m-%d}/{now:%Y%m%dT%H%M%S%f}.json"
    body = json.dumps(
        {"source": "master_data", "ingested_at": now.isoformat(), "records": records},
        ensure_ascii=False,
        default=str,
    )
    boto3.client("s3").put_object(
        Bucket=os.environ.get("RAW_BUCKET_NAME", "ghn-analytics-dev-raw"),
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return {"written": {"master_data": {"count": len(records), "key": key}}, "errors": {}}
