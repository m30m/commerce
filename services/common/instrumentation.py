"""FastAPI app factory with RED middleware, a structured access log, a
/metrics endpoint and an event-loop lag sampler that runs for the lifetime of
the process."""
import asyncio
import contextlib
import time
from typing import Awaitable, Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import config
from .logging_config import configure_logging
from .metrics import (
    EVENT_LOOP_LAG,
    HTTP_DURATION,
    HTTP_IN_PROGRESS,
    HTTP_REQUESTS,
)

Hook = Optional[Callable[[], Awaitable[None]]]


async def _sample_event_loop_lag(interval: float) -> None:
    """Sleep ``interval`` and record how much longer than ``interval`` it
    actually took to be rescheduled. When the event loop is busy this drift
    climbs, which is a useful signal of event-loop saturation."""
    loop = asyncio.get_event_loop()
    while True:
        start = loop.time()
        await asyncio.sleep(interval)
        drift = loop.time() - start - interval
        EVENT_LOOP_LAG.set(max(0.0, drift))


def create_app(
    service_name: str,
    startup: Hook = None,
    shutdown: Hook = None,
) -> FastAPI:
    logger = configure_logging(service_name, config.LOG_LEVEL)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        lag_task = asyncio.create_task(
            _sample_event_loop_lag(config.LOOP_LAG_INTERVAL)
        )
        if startup is not None:
            await startup()
        try:
            yield
        finally:
            lag_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await lag_task
            if shutdown is not None:
                await shutdown()

    app = FastAPI(title=service_name, lifespan=lifespan)

    @app.middleware("http")
    async def record_metrics(request: Request, call_next):
        # Route template (e.g. /products/{pid}) keeps label cardinality bounded.
        endpoint = request.url.path
        HTTP_IN_PROGRESS.inc()
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            if route is not None and getattr(route, "path", None):
                endpoint = route.path
            elapsed = time.perf_counter() - start
            HTTP_IN_PROGRESS.dec()
            HTTP_DURATION.labels(request.method, endpoint).observe(elapsed)
            HTTP_REQUESTS.labels(request.method, endpoint, status).inc()
            if endpoint not in ("/health", "/metrics"):
                logger.info(
                    "request",
                    extra={
                        "method": request.method,
                        "path": endpoint,
                        "status": status,
                        "duration_ms": round(elapsed * 1000, 2),
                        "client": request.client.host if request.client else None,
                    },
                )

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health")
    async def health() -> PlainTextResponse:
        return PlainTextResponse("ok")

    return app
