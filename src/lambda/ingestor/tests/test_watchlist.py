"""Tests for the Ingestor's order-watchlist logic.

The watchlist is the set of GHN order codes the Ingestor polls, and it
lives in DynamoDB because the GHN API cannot list orders. These tests
exercise the table-driven poll set, env-seed registration, and the
terminal-status archival using a fake DynamoDB client - no AWS account
or network needed.

The runner (run_tests.py) invokes bare test_* functions, so each test
manages its own state with an explicit context manager rather than
pytest fixtures.
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import handler  # noqa: E402
from handler import (  # noqa: E402
    WATCH_PK,
    _load_order_codes,
    _record_poll_results,
    _register_codes,
    _scan_watchlist,
    _terminal,
)


class FakeTable:
    """In-memory stand-in for the watchlist table (single-page data)."""

    def __init__(self, rows: list[dict] | None = None) -> None:
        self.items: dict[str, dict] = {row["sk"]: dict(row) for row in (rows or [])}
        self.updates: list[dict] = []
        self.puts: list[dict] = []

    # boto3 client surface ---------------------------------------------------
    def scan(self, **kwargs: object) -> dict:
        assert not kwargs.get("ExclusiveStartKey"), "test data is single-page"
        return {"Items": list(self.items.values()), "Count": len(self.items)}

    def update_item(self, **kwargs: object) -> dict:
        key = kwargs["Key"]
        row = self.items.setdefault(key["sk"], {"pk": key["pk"], "sk": key["sk"]})
        vals = kwargs["ExpressionAttributeValues"]
        row["last_status"] = vals[":s"]
        row["updated_at"] = vals[":t"]
        row["terminal"] = bool(vals[":x"])
        self.updates.append(dict(row))
        return {"Attributes": row}

    def put_item(self, **kwargs: object) -> None:
        item = dict(kwargs["Item"])
        self.items[item["sk"]] = item
        self.puts.append(item)


def _make_fake_client(table: FakeTable):
    class _Client:
        def __init__(self, table) -> None:
            self.table = table

        def scan(self, **kwargs):
            return self.table.scan(**kwargs)

        def update_item(self, **kwargs):
            return self.table.update_item(**kwargs)

        def put_item(self, **kwargs):
            return self.table.put_item(**kwargs)

    return _Client(table)


@contextmanager
def _install(table: FakeTable, env: dict[str, str]):
    """Patch the watchlist functions onto a fake table for the duration of the body.

    Deterministic teardown: env keys and handler globals are restored on
    exit regardless of how the body ends.
    """
    controlled_keys = set(env).union({"ORDER_WATCHLIST_TABLE", "GHN_ORDER_CODES"})
    saved_env = {k: os.environ.get(k) for k in controlled_keys}
    saved_clients = handler._clients
    saved_table_client = handler._table_client
    fake_client = _make_fake_client(table)
    try:
        for k in ("ORDER_WATCHLIST_TABLE", "GHN_ORDER_CODES"):
            os.environ.pop(k, None)
        for k, v in env.items():
            os.environ[k] = v
        handler._clients = {"dynamodb": fake_client}
        handler._table_client = lambda: fake_client
        yield
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        handler._clients = saved_clients
        handler._table_client = saved_table_client


def test_terminal_classification():
    assert _terminal("delivered")
    assert _terminal("cancel")
    assert _terminal("delivery_fail")
    assert _terminal("returned")
    assert not _terminal("ready_to_pick")
    assert not _terminal("transporting")
    assert not _terminal("teleported")


def test_scan_returns_terminal_flags():
    table = FakeTable(
        [
            {"pk": WATCH_PK, "sk": "A", "terminal": False},
            {"pk": WATCH_PK, "sk": "B", "terminal": True},
        ]
    )
    with _install(table, {"ORDER_WATCHLIST_TABLE": "wh"}):
        assert _scan_watchlist() == {"A": False, "B": True}


def test_env_codes_register_once_and_poll():
    table = FakeTable()
    with _install(table, {"ORDER_WATCHLIST_TABLE": "wh", "GHN_ORDER_CODES": "X,Y"}):
        assert _load_order_codes() == ["X", "Y"]
        # codes are now in the table (registered idempotently)
        assert set(table.items) == {"X", "Y"}
        # a second load must not duplicate them
        assert _load_order_codes() == ["X", "Y"]
        assert len(table.puts) >= 2


def test_terminal_rows_are_skipped_on_later_runs():
    table = FakeTable(
        [{"pk": WATCH_PK, "sk": "DONE1", "terminal": True, "last_status": "delivered"}]
    )
    with _install(table, {"ORDER_WATCHLIST_TABLE": "wh", "GHN_ORDER_CODES": "DONE1"}):
        # DONE1 is terminal in the table, so a stale env entry must not re-activate it
        assert _load_order_codes() == []


def test_poll_results_mark_terminal_codes():
    table = FakeTable([{"pk": WATCH_PK, "sk": "LIVE", "terminal": False}])
    with _install(table, {"ORDER_WATCHLIST_TABLE": "wh"}):
        _record_poll_results(["LIVE"], {"LIVE": "delivered"})
        assert table.items["LIVE"]["terminal"] is True
        assert table.items["LIVE"]["last_status"] == "delivered"
        # and now it drops out of the poll set
        assert _load_order_codes() == []


def test_non_terminal_poll_results_keep_polling():
    table = FakeTable([{"pk": WATCH_PK, "sk": "LIVE", "terminal": False}])
    with _install(table, {"ORDER_WATCHLIST_TABLE": "wh"}):
        _record_poll_results(["LIVE"], {"LIVE": "transporting"})
        assert table.items["LIVE"]["terminal"] is False
        assert _load_order_codes() == ["LIVE"]


def test_register_is_idempotent():
    table = FakeTable()
    with _install(table, {"ORDER_WATCHLIST_TABLE": "wh"}):
        _register_codes(["Z"])
        _register_codes(["Z"])
        assert len(table.items) == 1
        assert table.items["Z"]["terminal"] is False


def test_no_table_falls_back_to_env_only():
    with _install(FakeTable(), {"GHN_ORDER_CODES": "A,B"}):
        # ORDER_WATCHLIST_TABLE unset: poll set is the env seed, nothing written
        assert _load_order_codes() == ["A", "B"]
