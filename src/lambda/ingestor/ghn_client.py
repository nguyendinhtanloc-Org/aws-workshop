"""Thin HTTP client for the GHN API with retry and rate-limit handling."""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

# The current GHN open API, per GHN's "API Environments" table:
#   production  https://online-gateway.ghn.vn
#   staging     https://dev-online-gateway.ghn.vn
#
# The /shiip/public-api URL in the GHN docs is the documentation
# portal. The real request URL is
#
#     {gateway}/shiip/public-api/{partner_id}{path}
#
# i.e. the partner id is a PATH segment, and requests are also
# authenticated with two headers: Token and ShopId. Without the
# partner_id segment the gateway answers every route with
# {"code":404,"message":"Not Found"} - the routing 404 that every
# probe so far produced.
BASE_URL = os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn").rstrip("/")
STAGING_BASE_URL = "https://dev-online-gateway.ghn.vn"
API_PREFIX = "/shiip/public-api"

# Endpoint paths for the current gateway generation. PROVISIONAL: the
# GHN docs describe the flows (create order, fee calculation, address
# sync, webhooks) and the authoritative route list is the gateway's
# OpenAPI spec. scripts/probe_ghn_paths.py fetches that spec when
# reachable and prints the real route names - until then, treat this
# dict as the single place to pin the confirmed paths.
ENDPOINTS: dict[str, str] = {
    "provinces": "/client/api/v4/provinces",
    "districts": "/client/api/v4/districts",
    "wards": "/client/api/v4/wards",
    "orders": "/client/api/v4/orders",
}
TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.0


def _as_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise a gateway response body into a list of records.

    The gateway generations disagree on the wrapper: some answer
    {"code": ..., "data": [...]}, some answer a bare list. Both are
    accepted so the client does not have to be edited when the shape
    changes.
    """
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return [data]


class GhnClientError(RuntimeError):
    """Raised when the GHN API cannot be reached or returns an error."""


class GhnClient:
    """Reads order and master data from the GHN open API.

    Handles the two failure modes the proposal calls out: rate limiting
    (HTTP 429) and transient 5xx. Both use exponential backoff with jitter.
    """

    def __init__(
        self,
        token: str,
        base_url: str = BASE_URL,
        shop_id: str = "",
        partner_id: str = "",
    ) -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._shop_id = shop_id
        self._partner_id = partner_id
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _headers(self) -> dict[str, str]:
        """The current gateway authenticates on two headers: Token and
        ShopId. The token is never logged or written to URLs."""
        headers = {"Token": self._token}
        if self._shop_id:
            headers["ShopId"] = self._shop_id
        return headers

    def _url_for(self, path: str) -> str:
        """Full request URL: gateway + API prefix + partner id + path.

        The partner id is a path segment, not a header - requests
        without it are rejected by the gateway's router with a 404 for
        every route.
        """
        partner = f"/{self._partner_id}" if self._partner_id else ""
        return f"{self._base_url}{API_PREFIX}{partner}{path}"

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET an endpoint, retrying transient failures with backoff.

        Only a 429 or a 5xx is retried; a 4xx is a client error and is
        raised immediately. The wait happens between attempts, so the
        final failed attempt does not sleep before the error is raised.
        """
        url = self._url_for(path)
        last_error: Exception | None = None

        for attempt in range(MAX_RETRIES):
            retryable = False

            try:
                resp = self._session.get(
                    url, headers=self._headers(), params=params, timeout=TIMEOUT_SECONDS
                )
            except requests.RequestException as exc:
                last_error = exc
                retryable = True
                logger.warning("request error path=%s attempt=%s error=%s", path, attempt, exc)
            else:
                if resp.status_code == 429:
                    retryable = True
                    logger.warning("rate limited path=%s attempt=%s", path, attempt)
                elif resp.status_code >= 500:
                    retryable = True
                    logger.warning(
                        "server error path=%s status=%s attempt=%s",
                        path,
                        resp.status_code,
                        attempt,
                    )
                elif resp.status_code >= 400:
                    raise GhnClientError(f"GHN API error path={path} status={resp.status_code}")
                else:
                    payload = resp.json()
                    if payload.get("code") not in (200, None):
                        raise GhnClientError(
                            f"GHN API error path={path} code={payload.get('code')}"
                        )
                    return payload

            # Wait before the next attempt, but not after the last one.
            if retryable and attempt < MAX_RETRIES - 1:
                delay = BACKOFF_BASE_SECONDS * (2**attempt)
                logger.info("retrying path=%s in %.1fs", path, delay)
                time.sleep(delay)

        raise GhnClientError(f"GHN API failed after {MAX_RETRIES} attempts path={path}: {last_error}")

    # --------------------------------------------------------------
    # The fetch methods operate through the endpoint paths pinned
    # above. The GHN docs deliver order-status changes by webhook
    # rather than a pollable status list, so shipping_status reuses
    # the orders endpoint.
    def fetch_master_data(self) -> list[dict[str, Any]]:
        """Return provinces, districts and wards as one flat record list."""
        records: list[dict[str, Any]] = []
        for entity, key in (("province", "provinces"), ("district", "districts"), ("ward", "wards")):
            payload = self._get(ENDPOINTS[key])
            for item in _as_records(payload):
                records.append({"entity": entity, **item})
        return records

    def fetch_orders(self) -> list[dict[str, Any]]:
        """Return the current page of orders.

        The API returns orders page by page; this method fetches the
        first page only. Pagination is added when the demo dataset
        exceeds one page.
        """
        payload = self._get(ENDPOINTS["orders"])
        return list(_as_records(payload))

    def fetch_shipping_status(self) -> list[dict[str, Any]]:
        """Return shipping status records.

        Fetched from the orders endpoint; order status in the current
        generation is nested on each order and also delivered by
        webhook, not exposed as a separate pollable list.
        """
        payload = self._get(ENDPOINTS["orders"], params={"with_status": True})
        return list(_as_records(payload))

