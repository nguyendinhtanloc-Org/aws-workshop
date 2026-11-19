"""Execute SQL through Trino for Bronze, Silver and Gold jobs."""
from __future__ import annotations

import json
import os
from urllib.request import Request, urlopen


def execute_sql(sql: str) -> list:
    """Wait for the Trino query to finish, including Iceberg commit errors."""
    request = Request(
        os.environ.get("TRINO_ENDPOINT", "http://localhost:8080").rstrip("/") + "/v1/statement",
        data=sql.encode("utf-8"),
        headers={"X-Trino-User": "pipeline-writer"},
    )
    rows = []
    while True:
        with urlopen(request, timeout=60) as response:
            result = json.load(response)
        if "error" in result:
            raise RuntimeError(f'Trino: {result["error"]["message"]}')
        rows.extend(result.get("data", []))
        if "nextUri" not in result:
            return rows
        request = Request(result["nextUri"])
