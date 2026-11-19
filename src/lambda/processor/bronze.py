"""Archive each normalized order and its metadata in MinIO Bronze."""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from shared.trino_client import execute_sql


@lru_cache(maxsize=1)
def minio_client() -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def sql_string(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@lru_cache(maxsize=8)
def prepare_table(bucket: str) -> str:
    execute_sql("CREATE SCHEMA IF NOT EXISTS iceberg.bronze")
    table_name = os.environ.get("BRONZE_ICEBERG_TABLE", "ghn_events")
    if not re.fullmatch(r"[a-z][a-z0-9_]*", table_name):
        raise ValueError("BRONZE_ICEBERG_TABLE must be a lowercase SQL identifier")
    name = f"iceberg.bronze.{table_name}"
    location = sql_string(f"s3://{bucket}/tables/ghn_events")
    create_sql = f"""CREATE TABLE IF NOT EXISTS {name} (
        event_id varchar, order_code varchar, status varchar, event_type varchar,
        source varchar, event_date date, event_time timestamp(6) with time zone,
        ingest_date date, ingest_time timestamp(6) with time zone,
        raw_uri varchar, raw_sha256 varchar
    ) WITH (location = {location}, format = 'PARQUET', format_version = 2,
            partitioning = ARRAY['day(ingest_time)'])"""
    try:
        execute_sql(create_sql)
    except RuntimeError as exc:
        if "Cannot create a table on a non-empty location" not in str(exc):
            raise
        # A lost catalog can leave a valid Iceberg table in object storage.
        # Let Iceberg resolve its metadata version rather than creating over it.
        execute_sql(f"""CALL iceberg.system.register_table(
            schema_name => 'bronze', table_name => {sql_string(table_name)},
            table_location => {location})""")
    definition = execute_sql(f"SHOW CREATE TABLE {name}")[0][0]
    if f"s3://{bucket}/tables/ghn_events" not in definition:
        raise RuntimeError("Bronze Iceberg table points to a different bucket")
    return name


def write_order(bucket: str, order: dict[str, Any], message: dict[str, Any]) -> None:
    """Archive JSON, then commit its event to Iceberg through Trino."""

    attrs = message.get("messageAttributes") or {}
    source = attrs.get("source", {}).get("stringValue", "ghn_legacy")
    received_at = attrs.get("ingest_time", {}).get("stringValue")
    if received_at:
        ingest_time = datetime.fromisoformat(received_at).astimezone(UTC)
    else:
        ingest_time = datetime.fromtimestamp(int(message["attributes"]["SentTimestamp"]) / 1000, UTC)
    event_time = datetime.fromisoformat(str(order.get("updated_at") or order["created_at"]))
    event_time = (
        event_time.astimezone(UTC) if event_time.tzinfo else event_time.replace(tzinfo=UTC)
    )
    event_id = str(uuid5(NAMESPACE_URL, f'ghn-sqs:{message["messageId"]}'))
    stem = f'{event_time.date()}_{ingest_time:%Y%m%dT%H%M%S%fZ}_{event_id}'
    raw_key = f"raw/{stem}.json"
    raw = json.dumps(order, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    metadata = {
        "source": source,
        "order_code": str(order["order_code"]),
        "event_date": event_time.date(),
        "event_time": event_time,
        "ingest_date": ingest_time.date(),
        "ingest_time": ingest_time,
        "event_id": event_id,
        "raw_uri": f"s3://{bucket}/{raw_key}",
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "status": str(order.get("status", "")),
        "event_type": str(order.get("event_type", "")),
    }
    table = prepare_table(bucket)
    client = minio_client()
    client.put_object(Bucket=bucket, Key=raw_key, Body=raw, ContentType="application/json")
    values = []
    for key, value in metadata.items():
        literal = sql_string(value)
        if key in {"event_time", "ingest_time"}:
            literal = f"from_iso8601_timestamp({sql_string(value.isoformat())})"
        elif key in {"event_date", "ingest_date"}:
            literal = f"DATE {literal}"
        values.append(literal)
    columns = ", ".join(metadata)
    execute_sql(f"""MERGE INTO {table} t
        USING (VALUES ({', '.join(values)})) s ({columns})
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT ({columns})
        VALUES ({', '.join('s.' + key for key in metadata)})""")
