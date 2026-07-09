"""Prometheus metric definitions shared by every service.

The default prometheus_client registry already ships ProcessCollector (RSS via
``process_resident_memory_bytes``) and GCCollector (``python_gc_*``), so memory
and GC stats are covered for free. The metrics below add the RED surface plus
operational gauges: DB pool wait-time / in-use count, cache hit-rate,
event-loop lag, queue/semaphore depth and downstream call stats.
"""
from prometheus_client import Counter, Gauge, Histogram

# Latency buckets tuned for p50/p95/p99 resolution on a high-RPS API.
_LATENCY_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5,
    0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 7.5, 10.0,
)

# --- RED: Rate / Errors / Duration -----------------------------------------
HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests handled.",
    ["method", "endpoint", "status"],
)
HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request handling latency.",
    ["method", "endpoint"],
    buckets=_LATENCY_BUCKETS,
)
HTTP_IN_PROGRESS = Gauge(
    "http_requests_in_progress",
    "In-flight HTTP requests.",
)

# --- Downstream (service-to-service) calls ---------------------------------
DOWNSTREAM_REQUESTS = Counter(
    "downstream_requests_total",
    "Outbound requests to a downstream service.",
    ["target", "status"],
)
DOWNSTREAM_DURATION = Histogram(
    "downstream_request_duration_seconds",
    "Outbound request latency to a downstream service.",
    ["target"],
    buckets=_LATENCY_BUCKETS,
)

# --- DB connection pool (USE / saturation) ---------------------------------
DB_POOL_SIZE = Gauge(
    "db_pool_size",
    "Configured maximum size of the DB connection pool.",
)
DB_POOL_IN_USE = Gauge(
    "db_pool_in_use_connections",
    "Connections currently checked out of the pool.",
)
DB_POOL_WAIT = Histogram(
    "db_pool_wait_seconds",
    "Time spent waiting to acquire a connection from the pool.",
    buckets=(
        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
        0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    ),
)
DB_QUERY_DURATION = Histogram(
    "db_query_duration_seconds",
    "Time spent executing a DB query (excludes pool-acquire wait).",
    ["op"],
    buckets=_LATENCY_BUCKETS,
)
DB_QUERIES = Counter(
    "db_queries_total",
    "Total DB queries executed (per logical operation).",
    ["op"],
)

# --- Cache -----------------------------------------------------------------
CACHE_REQUESTS = Counter(
    "cache_requests_total",
    "Cache lookups by result.",
    ["result"],  # hit | miss
)

# --- Event loop / concurrency saturation -----------------------------------
EVENT_LOOP_LAG = Gauge(
    "event_loop_lag_seconds",
    "Observed asyncio event-loop scheduling delay (drift beyond a sleep).",
)
QUEUE_DEPTH = Gauge(
    "queue_depth",
    "Depth of a named in-process/redis queue.",
    ["name"],
)
SEMAPHORE_IN_USE = Gauge(
    "semaphore_in_use",
    "Slots currently held on a named semaphore/limiter.",
    ["name"],
)
