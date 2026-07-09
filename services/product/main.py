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
