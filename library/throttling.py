"""Helpers for API throttling and retry policies."""

from __future__ import annotations

import time
from typing import Callable

try:  # pragma: no cover - optional dependency
    import requests  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    requests = None  # type: ignore[assignment]


def retry_request(
    request_fn: Callable[[], requests.Response],
    *,
    retries: int = 3,
    backoff: float = 1.0,
) -> requests.Response:
    """Invoke *request_fn* with exponential backoff on throttling errors."""

    if requests is None:  # pragma: no cover - runtime guard
        raise RuntimeError("The 'requests' package is required for retry_request")

    attempt = 0
    while True:
        try:
            response = request_fn()
            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response else None
            if status not in {429, 500, 502, 503, 504}:
                raise
            if attempt >= retries:
                raise
            sleep_for = backoff * (2 ** attempt)
            retry_after = exc.response.headers.get("Retry-After") if exc.response else None
            if retry_after:
                try:
                    sleep_for = max(float(retry_after), sleep_for)
                except ValueError:
                    pass
            time.sleep(sleep_for)
            attempt += 1


__all__ = ["retry_request"]
