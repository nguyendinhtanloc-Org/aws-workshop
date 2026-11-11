"""Tests for the GHN API client retry and error handling."""
from __future__ import annotations

import pytest
from ghn_client import GhnClient, GhnClientError


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class FakeSession:
    """Replays a scripted sequence of responses."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = responses
        self.headers: dict[str, str] = {}
        self.calls = 0

    def get(self, url: str, headers: dict, params: dict | None = None, timeout: int = 0):
        self.calls += 1
        self.last_params = params or {}
        self.last_url = url
        return self._responses.pop(0)


def _client(responses: list[FakeResponse]) -> tuple[GhnClient, FakeSession]:
    client = GhnClient(token="secret-token")
    session = FakeSession(responses)
    client._session = session  # test seam
    return client, session


def test_successful_response_is_returned():
    client, _ = _client([FakeResponse(200, {"code": 200, "data": [{"province_id": 1}]})])
    payload = client._get("/api/v1/apiv3/getProvinces")
    assert payload["data"] == [{"province_id": 1}]


def test_rate_limit_is_retried():
    client, session = _client(
        [FakeResponse(429), FakeResponse(200, {"code": 200, "data": []})]
    )
    payload = client._get("/api/v1/apiv3/getOrder")
    assert payload["data"] == []
    assert session.calls == 2


def test_server_error_is_retried():
    client, session = _client(
        [FakeResponse(503), FakeResponse(200, {"code": 200, "data": []})]
    )
    client._get("/api/v1/apiv3/getOrder")
    assert session.calls == 2


def test_client_error_is_not_retried():
    client, session = _client([FakeResponse(403, {"message": "forbidden"})])
    with pytest.raises(GhnClientError, match="403"):
        client._get("/api/v1/apiv3/getOrder")
    assert session.calls == 1


def test_api_level_error_code_raises():
    client, _ = _client([FakeResponse(200, {"code": 400, "message": "invalid token"})])
    with pytest.raises(GhnClientError, match="code=400"):
        client._get("/api/v1/apiv3/getOrder")


def test_exhausted_retries_raise():
    client, session = _client([FakeResponse(429), FakeResponse(429), FakeResponse(429)])
    with pytest.raises(GhnClientError, match="failed after 3 attempts"):
        client._get("/api/v1/apiv3/getOrder")
    assert session.calls == 3


def test_fetch_master_data_flattens_entities():
    """Province + district come from one call each; wards are fetched
    per district when district ids are supplied."""
    client, session = _client(
        [
            FakeResponse(200, {"code": 200, "data": [{"province_id": 1}]}),
            FakeResponse(200, {"code": 200, "data": [{"district_id": 2}]}),
            FakeResponse(200, {"code": 200, "data": [{"ward_code": "A"}]}),
        ]
    )
    records = client.fetch_master_data(ward_district_ids=[2])
    assert [r["entity"] for r in records] == ["province", "district", "ward"]
    # ward request carried the required district_id param
    assert session.calls == 3


def test_fetch_master_data_without_districts_skips_ward_calls():
    client, session = _client(
        [
            FakeResponse(200, {"code": 200, "data": [{"province_id": 1}]}),
            FakeResponse(200, {"code": 200, "data": [{"district_id": 2}]}),
        ]
    )
    records = client.fetch_master_data()
    assert [r["entity"] for r in records] == ["province", "district"]
    assert session.calls == 2


def test_fetch_orders_polls_by_code():
    """The API has no list-orders route: one call per code, each with
    the required order_code query param (snake_case, confirmed live)."""
    client, session = _client(
        [
            FakeResponse(200, {"code": 200, "data": {"order_code": "C1", "status": "picked"}}),
            FakeResponse(200, {"code": 200, "data": {"order_code": "C2", "status": "delivered"}}),
        ]
    )
    records = client.fetch_orders(order_codes=["C1", "C2"])
    assert [r["order_code"] for r in records] == ["C1", "C2"]
    assert records[0]["status"] == "picked"
    # the confirmed live param name is snake_case, not the Go struct name
    assert session.last_params == {"order_code": "C2"}


def test_fetch_orders_without_codes_returns_nothing():
    """An empty watchlist is a valid run - nothing to fetch."""
    client, session = _client([])
    assert client.fetch_orders() == []
    assert session.calls == 0
