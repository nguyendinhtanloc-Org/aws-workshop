"""Thin HTTP client for the GHN API with retry and rate-limit handling."""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

BASE_URL = os.environ.get("GHN_BASE_URL", "https://online-gateway.ghn.vn").rstrip("/")
STAGING_BASE_URL = "https://dev-online-gateway.ghn.vn"
API_PREFIX = os.environ.get("GHN_API_PREFIX", "/shiip/public-api")

ENDPOINTS: dict[str, str] = {
    "provinces": "/master-data/province",
    "districts": "/master-data/district",
    "wards": "/master-data/ward",
    "orders": "/v2/shipping-order/detail",
    "shops": "/v2/shop/all",
    "create_order": "/v2/shipping-order/create",
    "update_order": "/v2/shipping-order/update",
    "cancel_orders": "/v2/switch-status/cancel",
}
TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.0


def _as_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise a gateway response body into a list of records."""
    data = payload.get("data", payload) if isinstance(payload, dict) else payload
    if data is None:
        return []
    if isinstance(data, list):
        return data
    return [data]


class GhnClientError(RuntimeError):
    """Raised when the GHN API cannot be reached or returns an error."""


class GhnClient:
    """Reads order and master data from the GHN open API."""

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
        """The current gateway authenticates on two headers: Token and ShopId."""
        headers = {"Token": self._token}
        if self._shop_id:
            headers["ShopId"] = self._shop_id
        return headers

    def _url_for(self, path: str) -> str:
        """Full request URL: gateway host + API prefix + endpoint path."""
        return f"{self._base_url}{API_PREFIX}{path}"

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """POST a JSON body, with the same retry/backoff policy as _get."""
        url = self._url_for(path)
        last_error: Exception | None = None
        for attempt in range(MAX_RETRIES):
            retryable = False
            try:
                resp = self._session.post(
                    url, headers=self._headers(), json=body, timeout=TIMEOUT_SECONDS
                )
            except requests.RequestException as exc:
                last_error = exc
                retryable = True
                logger.warning("request error path=%s attempt=%s error=%s", path, attempt, exc)
            else:
                if resp.status_code in (429, 500, 502, 503, 504):
                    retryable = True
                    logger.warning("retryable status=%s path=%s attempt=%s", resp.status_code, path, attempt)
                elif resp.status_code >= 400:
                    try:
                        detail = resp.json().get("message") or resp.text[:160]
                    except ValueError:
                        detail = resp.text[:160]
                    raise GhnClientError(f"GHN API error path={path} status={resp.status_code}: {detail}")
                else:
                    payload = dict(resp.json())
                    if payload.get("code") not in (200, None):
                        raise GhnClientError(f"GHN API error path={path} code={payload.get('code')}")
                    return payload
            if retryable and attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
        raise GhnClientError(f"GHN API failed after {MAX_RETRIES} attempts path={path}: {last_error}")

    def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Create a shipping order; return the response's data object."""
        response = self._post(ENDPOINTS["create_order"], payload)
        return dict(response.get("data") or {})

    def update_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Update the supplied fields of an existing GHN order."""
        return self._post(ENDPOINTS["update_order"], payload)

    def cancel_orders(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Cancel orders; callers must also inspect each data[].result."""
        return self._post(ENDPOINTS["cancel_orders"], payload)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET an endpoint, retrying transient failures with backoff."""
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
                    try:
                        detail = resp.json().get("message") or resp.text[:160]
                    except ValueError:
                        detail = resp.text[:160]
                    raise GhnClientError(f"GHN API error path={path} status={resp.status_code}: {detail}")
                else:
                    payload = dict(resp.json())
                    if payload.get("code") not in (200, None):
                        raise GhnClientError(
                            f"GHN API error path={path} code={payload.get('code')}"
                        )
                    return payload

            if retryable and attempt < MAX_RETRIES - 1:
                delay = BACKOFF_BASE_SECONDS * (2**attempt)
                logger.info("retrying path=%s in %.1fs", path, delay)
                time.sleep(delay)

        raise GhnClientError(f"GHN API failed after {MAX_RETRIES} attempts path={path}: {last_error}")

    def fetch_master_data(
        self, ward_district_ids: list[int] | None = None
    ) -> list[dict[str, Any]]:
        """Return provinces, districts and wards as one flat record list."""
        records: list[dict[str, Any]] = []
        for entity, key in (("province", "provinces"), ("district", "districts")):
            payload = self._get(ENDPOINTS[key])
            for item in _as_records(payload):
                records.append({"entity": entity, **item})
        for district_id in ward_district_ids or []:
            payload = self._get(ENDPOINTS["wards"], params={"district_id": district_id})
            for item in _as_records(payload):
                records.append({"entity": "ward", **item})
        return records

    def fetch_orders(self, order_codes: list[str] | None = None) -> list[dict[str, Any]]:
        """Return the shipping-order detail for each code in the watchlist."""
        codes = order_codes or []
        if not codes:
            return []
        records: list[dict[str, Any]] = []
        last_error: GhnClientError | None = None
        for code in codes:
            try:
                payload = self._get(ENDPOINTS["orders"], params={"order_code": code})
                records.extend(_as_records(payload))
            except GhnClientError as exc:
                last_error = exc
                logger.warning("failed to fetch order_code=%s: %s", code, exc)
        if not records and last_error is not None:
            raise last_error
        return records

    def fetch_shops(self, offset: int = 0, limit: int = 20) -> list[dict[str, Any]]:
        """List shops visible to this token; each ``_id`` is a ShopId."""
        payload = self._get(
            ENDPOINTS["shops"], params={"offset": offset, "limit": limit}
        )
        data = payload.get("data", {})
        if isinstance(data, dict):
            shops = data.get("shops", [])
            return [dict(shop) for shop in shops if isinstance(shop, dict)]
        return _as_records(payload)

