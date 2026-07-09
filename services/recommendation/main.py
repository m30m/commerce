"""recommendation — ranking service called by the gateway.

Computes a per-user recommendation list from recent cart activity plus the
catalog, with a small amount of in-process ranking work.
"""
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


app = create_app("recommendation", startup=_startup, shutdown=_shutdown)


def _score(popularity: int, price: float) -> float:
    # Simple ranking: popularity discounted by price.
    return popularity / (1.0 + price)


@app.get("/recommendations/{uid}")
async def recommend(uid: int, limit: int = 10) -> dict:
    # Global popularity from cart activity (cheap aggregate over an indexed col).
    popular = await db.fetch(
        "rec_popular",
        "SELECT product_id, COUNT(*) AS c FROM cart_items "
        "GROUP BY product_id ORDER BY c DESC LIMIT 50",
    )
    if not popular:
        return {"user_id": uid, "items": []}

    ids = [r["product_id"] for r in popular]
    counts = {r["product_id"]: r["c"] for r in popular}
    rows = await db.fetch(
        "rec_products",
        "SELECT id, name, price FROM products WHERE id = ANY($1::int[])",
        ids,
    )

    ranked = sorted(
        (
            {
                "id": r["id"],
                "name": r["name"],
                "price": float(r["price"]),
                "score": _score(counts.get(r["id"], 0), float(r["price"])),
            }
            for r in rows
        ),
        key=lambda x: x["score"],
        reverse=True,
    )
    return {"user_id": uid, "items": ranked[:limit]}
