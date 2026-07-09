"""Runtime configuration, sourced entirely from environment variables.

Operational knobs (pool sizes, cache TTLs, timeouts, retry counts) are exposed
here so they can be tuned per environment without editing service logic.
"""
import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


SERVICE_NAME = os.getenv("SERVICE_NAME", "service")

# Backing stores
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://eyebench:eyebench@postgres:5432/eyebench"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Connection pool sizing (product/cart/recommendation -> Postgres)
DB_POOL_MIN = _int("DB_POOL_MIN", 2)
DB_POOL_MAX = _int("DB_POOL_MAX", 20)

# Cache behaviour
CACHE_TTL = _int("CACHE_TTL", 60)

# Downstream service URLs
PRODUCT_URL = os.getenv("PRODUCT_URL", "http://product:8001")
CART_URL = os.getenv("CART_URL", "http://cart:8002")
RECOMMENDATION_URL = os.getenv("RECOMMENDATION_URL", "http://recommendation:8003")

# Outbound HTTP behaviour (timeout / retry knobs for downstream calls)
DOWNSTREAM_TIMEOUT = _float("DOWNSTREAM_TIMEOUT", 2.0)
# Separate, shorter budget for acquiring a slot from the connection pool so that
# pool exhaustion fails fast (visible backpressure) instead of consuming the
# full connect/read timeout while holding up the event loop.
DOWNSTREAM_POOL_TIMEOUT = _float("DOWNSTREAM_POOL_TIMEOUT", 0.5)
DOWNSTREAM_RETRIES = _int("DOWNSTREAM_RETRIES", 0)

# Event-loop lag sampler interval (seconds)
LOOP_LAG_INTERVAL = _float("LOOP_LAG_INTERVAL", 0.25)

# Access-log sampling. The per-request access log runs json.dumps + a blocking
# stdout write on the event-loop thread, so at high RPS it is pure throughput
# tax. Log 1 in every N successful (<400) requests; errors are always logged.
# Default 1 keeps every request (unchanged behaviour); raise it (e.g. 20) to
# trade log fidelity for loop time under load.
ACCESS_LOG_SAMPLE_N = _int("ACCESS_LOG_SAMPLE_N", 1)

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
