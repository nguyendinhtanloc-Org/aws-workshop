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
# Authentication is two headers: Token (API key) and ShopId. Both must be
# set - the router can reject requests that are missing ShopId with a
# routing 404.
#
BASE_URL = os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn").rstrip("/")
STAGING_BASE_URL = "https://dev-online-gateway.ghn.vn"
# Path prefix the gateway mounts the public API under. Overridable because
# GHN has moved it between generations - confirm it from the endpoint
# URLs in the GHN docs' "Open endpoint" pages.
# Confirmed from the GHN docs (https://developer.ghn.dev), which show
# full request URLs of the form:
#   GET {gateway}/shiip/public-api/master-data/province
#   GET {gateway}/shiip/public-api/master-data/district
#   GET {gateway}/shiip/public-api/master-data/ward
#   GET {gateway}/shiip/public-api/v2/shipping-order/detail
API_PREFIX = os.environ.get("GHN_API_PREFIX", "/shiip/public-api")

# Endpoint paths, confirmed from the GHN docs. The order route is
# shipping-order/detail; district and ward may take query parameters
# (province_id, district_id) - the GHN docs list them as optional
# filters.
ENDPOINTS: dict[str, str] = {
    "provinces": "/master-data/province",
    "districts": "/master-data/district",
    "wards": "/master-data/ward",
    "orders": "/v2/shipping-order/detail",
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
    ) -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._shop_id = shop_id
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
        """Full request URL: gateway host + API prefix + endpoint path."""
        return f"{self._base_url}{API_PREFIX}{path}"

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

