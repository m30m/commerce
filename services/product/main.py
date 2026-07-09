"""product — cache-fronted read service (Redis in front of Postgres)."""
from fastapi import HTTPException

from common import config
from common.cache import Cache
from common.db import Database
from common.instrumentation import create_app

db = Database()
cache = Cache()


async def _startup() -> None:
    await db.connect()
    await cache.connect()


async def _shutdown() -> None:
    await cache.disconnect()
    await db.disconnect()


app = create_app("product", startup=_startup, shutdown=_shutdown)


def _row_to_product(row) -> dict:
    data = dict(row)
    data["price"] = float(data["price"])
    return data


@app.get("/products/batch")
async def get_products_batch(ids: str) -> dict[int, dict]:
    """Look up many products in one call: cache mget for hits, a single DB query
    for the misses. Lets callers (e.g. cart) enrich a whole cart with one
    downstream request instead of one per line item.

    Declared before ``/products/{pid}`` so the literal path wins over the
    typed path parameter.
    """
    pids: list[int] = []
    seen: set[int] = set()
    for part in ids.split(","):
        part = part.strip()
        if not part:
            continue
        pid = int(part)
        if pid not in seen:
            seen.add(pid)
            pids.append(pid)
    if not pids:
        return {}

    cached = await cache.get_json_many([f"product:{pid}" for pid in pids])
    result: dict[int, dict] = {}
    missing: list[int] = []
    for pid, value in zip(pids, cached):
        if value is not None:
            result[pid] = value
        else:
            missing.append(pid)

    if missing:
        rows = await db.fetch(
            "product_batch",
            "SELECT id, name, price, description, category FROM products "
            "WHERE id = ANY($1::int[])",
            missing,
        )
        for row in rows:
            data = _row_to_product(row)
            result[data["id"]] = data
            await cache.set_json(f"product:{data['id']}", data, config.CACHE_TTL)

    return result


@app.get("/products/{pid}")
async def get_product(pid: int) -> dict:
    key = f"product:{pid}"
    cached = await cache.get_json(key)
    if cached is not None:
        return cached

    row = await db.fetchrow(
        "product_by_id",
        "SELECT id, name, price, description, category FROM products WHERE id = $1",
        pid,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="product not found")

    data = _row_to_product(row)
    await cache.set_json(key, data, config.CACHE_TTL)
    return data


@app.get("/products")
async def list_products(limit: int = 20, category: str | None = None) -> list[dict]:
    if category:
        rows = await db.fetch(
            "product_list_by_category",
            "SELECT id, name, price, description, category FROM products "
            "WHERE category = $1 ORDER BY id LIMIT $2",
            category,
            limit,
        )
    else:
        rows = await db.fetch(
            "product_list",
            "SELECT id, name, price, description, category FROM products "
            "ORDER BY id LIMIT $1",
            limit,
        )
    return [_row_to_product(r) for r in rows]
