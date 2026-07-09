"""Helper for instrumented service-to-service HTTP calls.

Wraps an httpx.AsyncClient and records downstream request rate/latency/status.
Timeout and retry behaviour are driven by config so they can be tuned per
environment without touching call sites.
"""
import time
from typing import Any, Optional

import httpx

from . import config
from .metrics import DOWNSTREAM_DURATION, DOWNSTREAM_REQUESTS


class DownstreamClient:
    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                config.DOWNSTREAM_TIMEOUT, pool=config.DOWNSTREAM_POOL_TIMEOUT
            ),
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def get_json(self, target: str, url: str, **kwargs: Any) -> Any:
        assert self._client is not None, "DownstreamClient.connect() not called"
        attempts = config.DOWNSTREAM_RETRIES + 1
        last_exc: Optional[Exception] = None
        for _ in range(attempts):
            start = time.perf_counter()
            try:
                resp = await self._client.get(url, **kwargs)
                DOWNSTREAM_DURATION.labels(target).observe(
                    time.perf_counter() - start
                )
                DOWNSTREAM_REQUESTS.labels(target, resp.status_code).inc()
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:  # noqa: BLE001 - recorded and retried
                DOWNSTREAM_DURATION.labels(target).observe(
                    time.perf_counter() - start
                )
                DOWNSTREAM_REQUESTS.labels(target, "error").inc()
                last_exc = exc
        assert last_exc is not None
        raise last_exc

    async def post_json(self, target: str, url: str, json: Any = None, **kwargs: Any) -> Any:
        assert self._client is not None, "DownstreamClient.connect() not called"
        start = time.perf_counter()
        try:
            resp = await self._client.post(url, json=json, **kwargs)
            DOWNSTREAM_DURATION.labels(target).observe(time.perf_counter() - start)
            DOWNSTREAM_REQUESTS.labels(target, resp.status_code).inc()
            resp.raise_for_status()
            return resp.json()
        except Exception:
            DOWNSTREAM_REQUESTS.labels(target, "error").inc()
            raise
