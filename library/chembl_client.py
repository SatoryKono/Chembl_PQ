"""Minimal ChEMBL REST API client with pagination support."""

from __future__ import annotations

import time
from typing import Dict, Iterator

try:  # pragma: no cover - imported lazily for optional dependency
    import requests  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - fallback for optional dependency
    requests = None  # type: ignore[assignment]

BASE_URL = "https://www.ebi.ac.uk/chembl/api/data"


def paged(
    endpoint: str,
    params: Dict[str, object] | None = None,
    limit: int = 1000,
    sleep: float = 0.0,
) -> Iterator[dict]:
    """Yield dictionaries from the ChEMBL API handling pagination."""

    if requests is None:  # pragma: no cover - runtime guard
        raise RuntimeError("The 'requests' package is required to use the ChEMBL client")

    query: Dict[str, object] = dict(params or {})
    query["limit"] = min(max(int(limit), 1), 1000)
    offset = 0

    while True:
        query["offset"] = offset
        response = requests.get(
            f"{BASE_URL}/{endpoint}.json", params=query, timeout=60
        )
        response.raise_for_status()
        payload = response.json()
        items = payload.get(endpoint) or payload.get("items") or []
        if not items:
            break
        for item in items:
            yield item
        offset += len(items)
        if sleep:
            time.sleep(sleep)


__all__ = ["paged", "BASE_URL"]
