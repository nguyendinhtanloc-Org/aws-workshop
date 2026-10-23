"""Canonical schemas for the GHN data layers."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Field:
    name: str
    dtype: str
    nullable: bool = True


ORDER_SCHEMA: tuple[Field, ...] = (
    Field("order_id", "string", nullable=False),
    Field("order_code", "string", nullable=False),
    Field("from_province_id", "int"),
    Field("from_district_id", "int"),
    Field("to_province_id", "int"),
    Field("to_district_id", "int"),
    Field("weight", "double"),
    Field("status", "string", nullable=False),
    Field("shipping_fee", "double"),
    Field("cod_amount", "double"),
    Field("created_at", "timestamp", nullable=False),
    Field("delivered_at", "timestamp"),
    Field("lead_time_hours", "double"),
)

MASTER_DATA_SCHEMA: tuple[Field, ...] = (
    Field("entity", "string", nullable=False),
    Field("province_id", "int"),
    Field("province_name", "string"),
    Field("district_id", "int"),
    Field("district_name", "string"),
    Field("ward_code", "string"),
    Field("ward_name", "string"),
)

STATUS_HISTORY_SCHEMA: tuple[Field, ...] = (
    Field("order_code", "string", nullable=False),
    Field("status", "string", nullable=False),
    Field("status_time", "timestamp"),
    Field("note", "string"),
)


def as_ddl(schema: tuple[Field, ...], table_name: str) -> str:
    """Render a schema as a Spark DDL string."""
    columns = []
    for field in schema:
        nullability = "" if field.nullable else " NOT NULL"
        columns.append(f"  {field.name} {field.dtype}{nullability}")
    body = ",\n".join(columns)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{body}\n)"
