"""Instrumented asyncpg connection pool.

Every connection acquisition records its wait time (``db_pool_wait_seconds``)
and adjusts the in-use gauge, so pool pressure is visible in the metrics.
"""
import contextlib
import time
from typing import Any, List, Optional

import asyncpg

from . import config
from .metrics import (
    DB_POOL_IN_USE,
    DB_POOL_SIZE,
    DB_POOL_WAIT,
    DB_QUERIES,
    DB_QUERY_DURATION,
)


class Database:
    def __init__(self) -> None:
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=config.DB_POOL_MIN,
            max_size=config.DB_POOL_MAX,
        )
        DB_POOL_SIZE.set(config.DB_POOL_MAX)

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    @contextlib.asynccontextmanager
    async def acquire(self):
        assert self._pool is not None, "Database.connect() not called"
        start = time.perf_counter()
        async with self._pool.acquire() as conn:
            DB_POOL_WAIT.observe(time.perf_counter() - start)
            DB_POOL_IN_USE.inc()
            try:
                yield conn
            finally:
                DB_POOL_IN_USE.dec()

    async def fetch(self, op: str, query: str, *args: Any) -> List[asyncpg.Record]:
        DB_QUERIES.labels(op).inc()
        async with self.acquire() as conn:
            with DB_QUERY_DURATION.labels(op).time():
                return await conn.fetch(query, *args)

    async def fetchrow(self, op: str, query: str, *args: Any) -> Optional[asyncpg.Record]:
        DB_QUERIES.labels(op).inc()
        async with self.acquire() as conn:
            with DB_QUERY_DURATION.labels(op).time():
                return await conn.fetchrow(query, *args)

    async def execute(self, op: str, query: str, *args: Any) -> str:
        DB_QUERIES.labels(op).inc()
        async with self.acquire() as conn:
            with DB_QUERY_DURATION.labels(op).time():
                return await conn.execute(query, *args)
