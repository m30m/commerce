"""product — cache-fronted read service (Redis in front of Postgres)."""
from fastapi import Body, HTTPException

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


def _product_fields(payload: dict) -> tuple[str, float, str, str]:
    """Validate a write payload. Every field is required: the create and update
    paths both write a complete row."""
    try:
        return (
            str(payload["name"]),
            float(payload["price"]),
            str(payload["description"]),
            str(payload["category"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="name, price, description and category are required",
        ) from exc


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


@app.post("/products", status_code=201)
async def create_product(payload: dict = Body(...)) -> dict:
    """Insert a new product and return the created row (including its id).

    The catalog is otherwise read-only here; this write path exists so callers
    can add products without touching Postgres directly.
    """
    name, price, description, category = _product_fields(payload)
    row = await db.fetchrow(
        "product_create",
        "INSERT INTO products (name, price, description, category) "
        "VALUES ($1, $2, $3, $4) "
        "RETURNING id, name, price, description, category",
        name,
        price,
        description,
        category,
    )
    return _row_to_product(row)


@app.put("/products/{pid}")
async def update_product(pid: int, payload: dict = Body(...)) -> dict:
    """Replace a product and return the updated row."""
    name, price, description, category = _product_fields(payload)
    row = await db.fetchrow(
        "product_update",
        "UPDATE products SET name = $2, price = $3, description = $4, category = $5 "
        "WHERE id = $1 "
        "RETURNING id, name, price, description, category",
        pid,
        name,
        price,
        description,
        category,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="product not found")

    # Invalidate after the commit rather than overwriting the key: a writer that
    # set the new value could still lose to a reader that missed just before the
    # UPDATE and lands its stale row afterwards. Dropping the key means the next
    # reader re-reads Postgres.
    await cache.delete(f"product:{pid}")
    return _row_to_product(row)


@app.delete("/products/{pid}")
async def delete_product(pid: int) -> dict:
    """Delete a product. Cart lines referencing it are removed by the
    ON DELETE CASCADE on cart_items.product_id, so no orphaned rows survive."""
    row = await db.fetchrow(
        "product_delete",
        "DELETE FROM products WHERE id = $1 RETURNING id",
        pid,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="product not found")

    await cache.delete(f"product:{pid}")
    return {"id": pid, "status": "deleted"}
