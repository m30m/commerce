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
import os
import random
import time
from collections import deque

import httpx

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
    while True:
        await asyncio.sleep(REPORT_INTERVAL_S)
        elapsed = time.perf_counter() - started_at
        snap = stats.snapshot()
        phase = "warmup" if elapsed < WARMUP_S else "steady"
        print(
            f"[load] t={elapsed:6.1f}s phase={phase} rps={RPS:.0f} "
            f"inflight={_inflight} total={snap['total']} errors={snap['errors']} "
            f"drops={snap['drops']} p50={snap['p50_ms']}ms "
            f"p95={snap['p95_ms']}ms p99={snap['p99_ms']}ms",
            flush=True,
        )


async def main() -> None:
    global _inflight
    print(
        f"[load] target={GATEWAY_URL} rps={RPS} read_frac={READ_FRACTION} "
        f"zipf_s={ZIPF_S} duration={'inf' if DURATION_S == 0 else DURATION_S}",
        flush=True,
    )
    interval = 1.0 / RPS
    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_S,
        limits=httpx.Limits(max_connections=MAX_INFLIGHT, max_keepalive_connections=200),
    ) as client:
        started_at = time.perf_counter()
        asyncio.create_task(_reporter(started_at))
        loop = asyncio.get_event_loop()
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
            # If sleep <= 0 we are behind schedule: keep firing immediately
            # (open-loop) rather than slowing the arrival process.

        print(f"[load] finished: {stats.snapshot()}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
