"""Minimal Instagram Graph API client (Instagram API with Instagram Login).

Only GETs. The token goes in the Authorization header, never in a URL we build, and is
stripped from Meta's own paging URLs before we follow them.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx

from .config import GRAPH_HOST
from .redact import strip_secret_params

# Graph error codes that mean "slow down": app/user/page-level throttling.
RATE_LIMIT_CODES = frozenset({4, 17, 32, 613, 80002})
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
# Response headers worth keeping as capture metadata.
KEEP_HEADERS = (
    "date",
    "content-type",
    "x-app-usage",
    "x-business-use-case-usage",
    "x-fb-trace-id",
    "x-fb-rev",
    "x-fb-request-id",
    "facebook-api-version",
)


class GraphError(RuntimeError):
    def __init__(self, response: httpx.Response):
        self.response = response
        try:
            err = response.json().get("error", {})
        except ValueError:
            err = {}
        self.code = err.get("code")
        super().__init__(
            f"Graph API {response.status_code}: {err.get('message', response.text[:200])}"
        )


class GraphClient:
    def __init__(
        self,
        token: str,
        api_version: str,
        *,
        transport: httpx.BaseTransport | None = None,
        max_retries: int = 5,
        min_interval: float = 0.25,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.token = token
        self.base = f"{GRAPH_HOST}/{api_version}/"
        self._http = httpx.Client(
            headers={"Authorization": f"Bearer {token}", "User-Agent": "comment-guard/0.1"},
            timeout=30.0,
            transport=transport,
        )
        self.max_retries = max_retries
        self.min_interval = min_interval
        self._sleep = sleep
        self._last = 0.0
        self.requests = 0
        # Called with every response (including failures) so the caller can land it.
        self.on_response: Callable[[httpx.Response, float], None] | None = None

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> GraphClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- requests -----------------------------------------------------------
    def get(self, path_or_url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        url = path_or_url if path_or_url.startswith("http") else self.base + path_or_url
        url = strip_secret_params(url)
        for attempt in range(self.max_retries + 1):
            self._throttle()
            started = time.monotonic()
            resp = self._http.get(url, params=params)
            self.requests += 1
            elapsed_ms = (time.monotonic() - started) * 1000
            if self.on_response:
                self.on_response(resp, elapsed_ms)
            if resp.is_success:
                return resp
            if attempt < self.max_retries and self._retryable(resp):
                self._sleep(min(2**attempt * 2.0, 300.0))
                continue
            raise GraphError(resp)
        raise AssertionError("unreachable")  # pragma: no cover

    def pages(self, path: str, params: dict[str, Any] | None = None) -> Iterator[httpx.Response]:
        """Yield every page of a cursor-paginated edge, following ``paging.next``."""
        resp = self.get(path, params)
        while True:
            yield resp
            nxt = resp.json().get("paging", {}).get("next")
            if not nxt:
                return
            resp = self.get(nxt)

    # -- helpers ------------------------------------------------------------
    def _throttle(self) -> None:
        wait = self._last + self.min_interval - time.monotonic()
        if wait > 0:
            self._sleep(wait)
        self._last = time.monotonic()

    @staticmethod
    def _retryable(resp: httpx.Response) -> bool:
        if resp.status_code in RETRY_STATUS:
            return True
        return GraphError(resp).code in RATE_LIMIT_CODES


def refresh_long_lived_token(
    token: str, *, transport: httpx.BaseTransport | None = None
) -> tuple[str, int]:
    """Exchange a >=24h-old long-lived token for a fresh 60-day one.

    Returns ``(new_token, expires_in_seconds)``.
    """
    with httpx.Client(timeout=30.0, transport=transport) as http:
        resp = http.get(
            f"{GRAPH_HOST}/refresh_access_token",
            params={"grant_type": "ig_refresh_token", "access_token": token},
        )
    if not resp.is_success:
        raise GraphError(resp)
    body = resp.json()
    return body["access_token"], int(body.get("expires_in", 0))
