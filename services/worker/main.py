"""worker — small background/async task tier.

Periodically recomputes a "trending products" list from cart activity and
publishes it to the cache, and drains a Redis task queue. It exposes the same
/metrics + /health surface as the web services so Prometheus scrapes it and the
worker's own event-loop lag / queue depth are observable.
"""
import asyncio
import contextlib
import logging
import os

from common.cache import Cache
from common.db import Database
from common.instrumentation import create_app
from common.metrics import QUEUE_DEPTH

logger = logging.getLogger("worker")

db = Database()
cache = Cache()

TRENDING_KEY = "trending:products"
TASK_QUEUE = "worker:tasks"
REFRESH_INTERVAL = int(os.getenv("WORKER_REFRESH_INTERVAL", "5"))

_bg_task: asyncio.Task | None = None


async def _refresh_trending() -> None:
    rows = await db.fetch(
        "worker_trending",
        "SELECT product_id, COUNT(*) AS c FROM cart_items "
        "GROUP BY product_id ORDER BY c DESC LIMIT 20",
    )
    trending = [{"product_id": r["product_id"], "count": r["c"]} for r in rows]
    await cache.set_json(TRENDING_KEY, trending, ttl=REFRESH_INTERVAL * 4)


async def _drain_tasks() -> None:
    # Non-blocking drain of any queued background tasks; records backlog depth.
    depth = await cache.client.llen(TASK_QUEUE)
    QUEUE_DEPTH.labels(TASK_QUEUE).set(depth)
    while True:
        item = await cache.client.rpop(TASK_QUEUE)
        if item is None:
            break
        # Process the queued task (currently a no-op placeholder).
    QUEUE_DEPTH.labels(TASK_QUEUE).set(0)


async def _run_loop() -> None:
    while True:
        try:
            await _refresh_trending()
            await _drain_tasks()
        except Exception:  # noqa: BLE001 - keep the loop alive
            logger.exception("worker iteration failed")
        await asyncio.sleep(REFRESH_INTERVAL)


async def _startup() -> None:
    global _bg_task
    await db.connect()
    await cache.connect()
    _bg_task = asyncio.create_task(_run_loop())


async def _shutdown() -> None:
    if _bg_task is not None:
        _bg_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _bg_task
    await cache.disconnect()
    await db.disconnect()


app = create_app("worker", startup=_startup, shutdown=_shutdown)
