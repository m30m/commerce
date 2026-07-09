"""Instrumented Redis cache wrapper.

Records every lookup as a hit or a miss (``cache_requests_total``), which is
the source of the cache hit-rate signal used to trace cache-degradation causal
chains later.
"""
import json
from typing import Any, Optional

import redis.asyncio as redis

from . import config
from .metrics import CACHE_REQUESTS


class Cache:
    def __init__(self) -> None:
        self._client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self._client = redis.from_url(config.REDIS_URL, decode_responses=True)
        await self._client.ping()

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    @property
    def client(self) -> redis.Redis:
        assert self._client is not None, "Cache.connect() not called"
        return self._client

    async def get_json(self, key: str) -> Optional[Any]:
        value = await self.client.get(key)
        if value is None:
            CACHE_REQUESTS.labels("miss").inc()
            return None
        CACHE_REQUESTS.labels("hit").inc()
        return json.loads(value)

    async def set_json(self, key: str, value: Any, ttl: int) -> None:
        await self.client.set(key, json.dumps(value), ex=ttl)
