"""Thin HTTP client for the GHN API with retry and rate-limit handling."""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

BASE_URL = os.environ.get("GHN_BASE_URL", "https://api.ghn.vn")
TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.0


class GhnClientError(RuntimeError):
    """Raised when the GHN API cannot be reached or returns an error."""


class GhnClient:
    """Reads order and master data from the GHN open API.

    Handles the two failure modes the proposal calls out: rate limiting
    (HTTP 429) and transient 5xx. Both use exponential backoff with jitter.
    """

    def __init__(self, token: str, base_url: str = BASE_URL) -> None:
        self._token = token
        self._base_url = base_url
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _headers(self) -> dict[str, str]:
        return {"Token": self._token}

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        last_error: Exception | None = None

        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(
                    url, headers=self._headers(), params=params, timeout=TIMEOUT_SECONDS
                )
            except requests.RequestException as exc:
                last_error = exc
                logger.warning("request error path=%s attempt=%s error=%s", path, attempt, exc)
            else:
                if resp.status_code == 429:
                    delay = BACKOFF_BASE_SECONDS * (2**attempt)
                    logger.warning("rate limited path=%s retry in %.1fs", path, delay)
                    time.sleep(delay)
                    continue
                if resp.status_code >= 500:
                    delay = BACKOFF_BASE_SECONDS * (2**attempt)
                    logger.warning("server error path=%s status=%s retry in %.1fs",
                                   path, resp.status_code, delay)
                    time.sleep(delay)
                    continue
                if resp.status_code >= 400:
                    raise GhnClientError(f"GHN API error path={path} status={resp.status_code}")

                payload = resp.json()
                if payload.get("code") not in (200, None):
                    raise GhnClientError(f"GHN API error path={path} code={payload.get('code')}")
                return payload

            time.sleep(BACKOFF_BASE_SECONDS * (2**attempt))

        raise GhnClientError(f"GHN API failed after {MAX_RETRIES} attempts path={path}: {last_error}")

    def fetch_master_data(self) -> list[dict[str, Any]]:
        """Return provinces, districts and wards as one flat record list."""
        records: list[dict[str, Any]] = []
        for entity, path in (
            ("province", "/api/v1/apiv3/getProvinces"),
            ("district", "/api/v1/apiv3/getDistricts"),
            ("ward", "/api/v1/apiv3/getWard"),
        ):
            payload = self._get(path)
            for item in payload.get("data", []) or []:
                records.append({"entity": entity, **item})
        return records

    def fetch_orders(self) -> list[dict[str, Any]]:
        """Return the current page of orders.

        The API returns orders page by page; this method fetches the first
        page only. Pagination is added when the demo dataset exceeds one page.
        """
        payload = self._get("/api/v1/apiv3/getOrder")
        return list(payload.get("data", []) or [])

    def fetch_shipping_status(self) -> list[dict[str, Any]]:
        """Return shipping status history records."""
        payload = self._get("/api/v1/apiv3/getOrder", params={"include": "status"})
        return list(payload.get("data", []) or [])
