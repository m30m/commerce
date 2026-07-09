"""Open-loop, constant-arrival-rate load generator.

Design notes:

* OPEN LOOP. Requests are launched on a fixed wall-clock schedule (one every
  1/RPS seconds) and fired as detached tasks. Arrivals never wait for prior
  responses, so when the system slows down we keep sending and the latency tail
  is measured accurately (avoids coordinated omission).

* ZIPFIAN KEYS. Product and user ids are drawn from a bounded Zipf
  distribution, so hot-key / cache-eviction effects show up.

* READ-HEAVY MIX. ~90% reads / ~10% writes.

Everything is tunable via environment variables (see the constants below).
Latency percentiles are reported periodically from the client's perspective.
"""
import asyncio
import bisect
import json
import logging
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timezone

import httpx


class _JsonFormatter(logging.Formatter):
    """Emit each record as a JSON line matching the services' log schema."""

    _RESERVED = set(
        vars(logging.LogRecord("", 0, "", 0, "", None, None)).keys()
    ) | {"message", "asctime", "taskName"}

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": "loadgen",
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = value
        return json.dumps(payload, default=str)


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
# httpx logs one line per request at INFO; quiet it to avoid flooding.
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("loadgen")

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://gateway:8000")
RPS = float(os.getenv("RPS", "50"))
DURATION_S = float(os.getenv("DURATION_S", "0"))  # 0 = run forever
READ_FRACTION = float(os.getenv("READ_FRACTION", "0.9"))
NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "1000"))
NUM_USERS = int(os.getenv("NUM_USERS", "500"))
ZIPF_S = float(os.getenv("ZIPF_S", "1.1"))  # skew; >1 = more concentrated
WARMUP_S = float(os.getenv("WARMUP_S", "10"))
REPORT_INTERVAL_S = float(os.getenv("REPORT_INTERVAL_S", "10"))
REQUEST_TIMEOUT_S = float(os.getenv("REQUEST_TIMEOUT_S", "10"))
# Safety cap on concurrent in-flight requests. Kept large so we stay open-loop;
# only a genuinely collapsing system hits it, and overflow is recorded as a drop
# rather than being allowed to blow up generator memory.
MAX_INFLIGHT = int(os.getenv("MAX_INFLIGHT", "5000"))


class ZipfSampler:
    """Bounded Zipf sampler over ids 1..n via a precomputed CDF."""

    def __init__(self, n: int, s: float) -> None:
        weights = [1.0 / (i ** s) for i in range(1, n + 1)]
        total = sum(weights)
        cdf = []
        acc = 0.0
        for w in weights:
            acc += w / total
            cdf.append(acc)
        self._cdf = cdf
        self._n = n

    def sample(self) -> int:
        return bisect.bisect_left(self._cdf, random.random()) + 1


class Stats:
    def __init__(self, window: int = 20000) -> None:
        self.latencies: deque[float] = deque(maxlen=window)
        self.total = 0
        self.errors = 0
        self.drops = 0

    def record(self, elapsed: float, ok: bool) -> None:
        self.total += 1
        if ok:
            self.latencies.append(elapsed)
        else:
            self.errors += 1

    def snapshot(self) -> dict:
        lat = sorted(self.latencies)
        def pct(p: float) -> float:
            if not lat:
                return 0.0
            idx = min(len(lat) - 1, int(p / 100.0 * len(lat)))
            return lat[idx]
        return {
            "total": self.total,
            "errors": self.errors,
            "drops": self.drops,
            "p50_ms": round(pct(50) * 1000, 1),
            "p95_ms": round(pct(95) * 1000, 1),
            "p99_ms": round(pct(99) * 1000, 1),
        }


products = ZipfSampler(NUM_PRODUCTS, ZIPF_S)
users = ZipfSampler(NUM_USERS, ZIPF_S)
stats = Stats()
_inflight = 0
# Worst schedule lag (seconds behind the fixed arrival schedule) seen since the
# last report. Surfaced by the reporter so "the generator can't keep up" is
# visible instead of silent.
_max_lag = 0.0


def _next_request() -> tuple[str, str, dict | None]:
    """Pick the next (method, url, json) to issue based on the workload mix."""
    if random.random() < READ_FRACTION:
        # Reads: mostly the home aggregate, some direct product lookups.
        if random.random() < 0.6:
            return "GET", f"{GATEWAY_URL}/home/{users.sample()}", None
        return "GET", f"{GATEWAY_URL}/products/{products.sample()}", None
    # Writes: add a hot product to a user's cart.
    uid = users.sample()
    body = {"product_id": products.sample(), "qty": random.randint(1, 3)}
    return "POST", f"{GATEWAY_URL}/carts/{uid}/items", body


async def _issue(client: httpx.AsyncClient, method: str, url: str, body: dict | None) -> None:
    global _inflight
    _inflight += 1
    start = time.perf_counter()
    ok = False
    try:
        resp = await client.request(method, url, json=body)
        ok = resp.status_code < 500
    except Exception:
        ok = False
    finally:
        stats.record(time.perf_counter() - start, ok)
        _inflight -= 1


async def _reporter(started_at: float) -> None:
    global _max_lag
    while True:
        await asyncio.sleep(REPORT_INTERVAL_S)
        try:
            elapsed = time.perf_counter() - started_at
            snap = stats.snapshot()
            phase = "warmup" if elapsed < WARMUP_S else "steady"
            lag = _max_lag
            _max_lag = 0.0
            # Effective RPS actually achieved this window (vs the requested RPS).
            achieved = round(snap["total"] / elapsed, 1) if elapsed > 0 else 0.0
            logger.info(
                "load report",
                extra={
                    "elapsed_s": round(elapsed, 1),
                    "phase": phase,
                    "rps": RPS,
                    "achieved_rps": achieved,
                    "behind_s": round(lag, 3),
                    "inflight": _inflight,
                    "total": snap["total"],
                    "errors": snap["errors"],
                    "drops": snap["drops"],
                    "p50_ms": snap["p50_ms"],
                    "p95_ms": snap["p95_ms"],
                    "p99_ms": snap["p99_ms"],
                },
            )
            # Loud, throttled warning when we can't sustain the requested rate.
            if lag > 1.0 or snap["drops"] > 0:
                logger.warning(
                    "load generator falling behind requested RPS",
                    extra={
                        "behind_s": round(lag, 3),
                        "drops": snap["drops"],
                        "inflight": _inflight,
                        "requested_rps": RPS,
                        "achieved_rps": achieved,
                    },
                )
        except Exception:
            # A crash in the reporter must never silently stop reporting.
            logger.exception("reporter iteration failed")


async def main() -> None:
    global _inflight, _max_lag
    logger.info(
        "load starting",
        extra={
            "target": GATEWAY_URL,
            "rps": RPS,
            "read_fraction": READ_FRACTION,
            "zipf_s": ZIPF_S,
            "duration_s": DURATION_S or None,
        },
    )
    interval = 1.0 / RPS
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_S,
        limits=httpx.Limits(max_connections=MAX_INFLIGHT, max_keepalive_connections=200),
    ) as client:
        started_at = time.perf_counter()
        loop = asyncio.get_event_loop()
        # Surface exceptions from detached tasks instead of asyncio swallowing
        # them into "Task exception was never retrieved" noise (or nothing).
        loop.set_exception_handler(
            lambda _loop, ctx: logger.error(
                "unhandled task exception",
                extra={"detail": ctx.get("message", str(ctx.get("exception")))},
            )
        )
        asyncio.create_task(_reporter(started_at))
        next_at = loop.time()
        while True:
            if DURATION_S and (time.perf_counter() - started_at) >= DURATION_S:
                break
            if _inflight < MAX_INFLIGHT:
                method, url, body = _next_request()
                asyncio.create_task(_issue(client, method, url, body))
            else:
                stats.drops += 1
            # Advance the schedule by a fixed step regardless of completion time.
            next_at += interval
            sleep = next_at - loop.time()
            if sleep > 0:
                await asyncio.sleep(sleep)
            else:
                # Behind schedule (the server or our own CPU can't keep up). Stay
                # open-loop and keep firing, but ALWAYS yield to the event loop —
                # otherwise this tight loop starves the in-flight requests and the
                # reporter, pinning the CPU at 100% with no output. That livelock
                # is what looked like the generator "silently dying".
                _max_lag = max(_max_lag, -sleep)
                await asyncio.sleep(0)

        logger.info("load finished", extra=stats.snapshot())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        # Never exit silently: a fatal error must leave a log line behind.
        logger.exception("load generator crashed")
        raise
