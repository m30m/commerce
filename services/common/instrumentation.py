"""FastAPI app factory with RED middleware, a structured access log, a
/metrics endpoint and an event-loop lag sampler that runs for the lifetime of
the process."""
import asyncio
import contextlib
import logging
import time
from typing import Awaitable, Callable, Optional

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse, PlainTextResponse, Response
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

_SKIP_LOG_ENDPOINTS = ("/health", "/metrics")


class MetricsMiddleware:
    """Pure-ASGI RED middleware + sampled structured access log.

    Implemented as raw ASGI (not Starlette's ``BaseHTTPMiddleware``) so it does
    not wrap every request in an anyio memory-object-stream and task group,
    which is the dominant per-request overhead on a busy single event loop.
    """

    def __init__(self, app, logger: logging.Logger) -> None:
        self.app = app
        self.logger = logger
        self._log_counter = 0

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        path = scope["path"]
        status = 500

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        HTTP_IN_PROGRESS.inc()
        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            # Route template (e.g. /products/{pid}) keeps label cardinality
            # bounded; the router populates scope["route"] during dispatch.
            route = scope.get("route")
            endpoint = getattr(route, "path", None) or path
            elapsed = time.perf_counter() - start
            HTTP_IN_PROGRESS.dec()
            HTTP_DURATION.labels(method, endpoint).observe(elapsed)
            HTTP_REQUESTS.labels(method, endpoint, status).inc()
            if endpoint not in _SKIP_LOG_ENDPOINTS and self._should_log(status):
                client = scope.get("client")
                # Logs carry both: "route" is the bounded template that lines up
                # with the metric labels, "path" is the concrete URL the client
                # asked for, so a log line identifies the exact request. The raw
                # path stays out of the metric labels — it is unbounded.
                self.logger.info(
                    "request",
                    extra={
                        "method": method,
                        "path": path,
                        "route": endpoint,
                        "status": status,
                        "duration_ms": round(elapsed * 1000, 2),
                        "client": client[0] if client else None,
                    },
                )

    def _should_log(self, status: int) -> bool:
        # Always surface errors; sample the successful firehose.
        if status >= 400:
            return True
        n = config.ACCESS_LOG_SAMPLE_N
        if n <= 1:
            return True
        # Single event-loop thread → a plain counter needs no locking.
        self._log_counter += 1
        return self._log_counter % n == 0


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

    # orjson (C extension) serialises responses several times faster than the
    # stdlib json module Starlette uses by default.
    app = FastAPI(
        title=service_name,
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )
    app.add_middleware(MetricsMiddleware, logger=logger)

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health")
    async def health() -> PlainTextResponse:
        return PlainTextResponse("ok")

    return app
