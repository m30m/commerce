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
        # One connection pool per downstream target rather than a single shared
        # pool. httpcore assigns pending requests to connections by scanning the
        # whole pool (O(pending * connections)) on every pool state change, and
        # for each candidate it compares request/connection Origin. A single
        # pool shared across product/cart/recommendation therefore makes every
        # scan pay Origin-equality checks against connections belonging to other
        # services. Splitting per target keeps each pool small and single-origin,
        # so the scan is cheaper and never compares across services.
        self._clients: dict[str, httpx.AsyncClient] = {}
        self._connected = False

    def _new_client(self) -> httpx.AsyncClient:
        # keepalive == max_connections: no churn from connections above the
        # keepalive ceiling being opened and immediately torn down under load,
        # which would otherwise trigger extra pool-reassignment scans.
        return httpx.AsyncClient(
            timeout=httpx.Timeout(
                config.DOWNSTREAM_TIMEOUT, pool=config.DOWNSTREAM_POOL_TIMEOUT
            ),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=50),
        )

    async def connect(self) -> None:
        self._connected = True

    def _client_for(self, target: str) -> httpx.AsyncClient:
        client = self._clients.get(target)
        if client is None:
            client = self._new_client()
            self._clients[target] = client
        return client

    async def disconnect(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()

    async def _get(self, target: str, url: str, **kwargs: Any) -> httpx.Response:
        assert self._connected, "DownstreamClient.connect() not called"
        client = self._client_for(target)
        attempts = config.DOWNSTREAM_RETRIES + 1
        last_exc: Optional[Exception] = None
        for _ in range(attempts):
            start = time.perf_counter()
            try:
                resp = await client.get(url, **kwargs)
                DOWNSTREAM_DURATION.labels(target).observe(
                    time.perf_counter() - start
                )
                DOWNSTREAM_REQUESTS.labels(target, resp.status_code).inc()
                resp.raise_for_status()
                return resp
            except Exception as exc:  # noqa: BLE001 - recorded and retried
                DOWNSTREAM_DURATION.labels(target).observe(
                    time.perf_counter() - start
                )
                DOWNSTREAM_REQUESTS.labels(target, "error").inc()
                last_exc = exc
        assert last_exc is not None
        raise last_exc

    async def get_json(self, target: str, url: str, **kwargs: Any) -> Any:
        resp = await self._get(target, url, **kwargs)
        return resp.json()

    async def get_raw(self, target: str, url: str, **kwargs: Any) -> tuple[bytes, str]:
        """Fetch a downstream response as raw bytes + content-type, skipping the
        decode/re-encode round-trip for pure pass-through endpoints."""
        resp = await self._get(target, url, **kwargs)
        return resp.content, resp.headers.get("content-type", "application/json")

    async def _write(
        self, method: str, target: str, url: str, json: Any = None, **kwargs: Any
    ) -> Any:
        """Shared body for the write verbs.

        Deliberately not retried, unlike ``_get``: a retry that follows a lost
        response would replay a write the upstream may already have applied.
        """
        assert self._connected, "DownstreamClient.connect() not called"
        client = self._client_for(target)
        start = time.perf_counter()
        try:
            resp = await client.request(method, url, json=json, **kwargs)
            DOWNSTREAM_DURATION.labels(target).observe(time.perf_counter() - start)
            DOWNSTREAM_REQUESTS.labels(target, resp.status_code).inc()
            resp.raise_for_status()
            return resp.json()
        except Exception:
            DOWNSTREAM_REQUESTS.labels(target, "error").inc()
            raise

    async def post_json(self, target: str, url: str, json: Any = None, **kwargs: Any) -> Any:
        return await self._write("POST", target, url, json=json, **kwargs)

    async def put_json(self, target: str, url: str, json: Any = None, **kwargs: Any) -> Any:
        return await self._write("PUT", target, url, json=json, **kwargs)

    async def delete_json(self, target: str, url: str, **kwargs: Any) -> Any:
        return await self._write("DELETE", target, url, **kwargs)
