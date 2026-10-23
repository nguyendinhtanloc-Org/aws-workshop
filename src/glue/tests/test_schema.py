"""Tests for schema DDL rendering."""
from __future__ import annotations

from lib.schema import ORDER_SCHEMA, as_ddl


def test_ddl_marks_required_fields_not_null():
    ddl = as_ddl(ORDER_SCHEMA, "orders")
    assert "order_code string NOT NULL" in ddl
    assert "weight double" in ddl
    assert "delivered_at timestamp" in ddl


def test_ddl_uses_table_name():
    assert "CREATE TABLE IF NOT EXISTS ghn.orders" in as_ddl(ORDER_SCHEMA, "ghn.orders")
