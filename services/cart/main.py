"""cart — read/write service; calls product to enrich line items."""
import asyncpg
from fastapi import Body, HTTPException

from common import config
from common.db import Database
from common.downstream import DownstreamClient
from common.instrumentation import create_app

db = Database()
downstream = DownstreamClient()


async def _startup() -> None:
    await db.connect()
    await downstream.connect()


async def _shutdown() -> None:
    await downstream.disconnect()
    await db.disconnect()


app = create_app("cart", startup=_startup, shutdown=_shutdown)


@app.get("/carts/{uid}")
async def get_cart(uid: int) -> dict:
    rows = await db.fetch(
        "cart_items",
        "SELECT product_id, qty FROM cart_items WHERE user_id = $1 "
        "ORDER BY added_at LIMIT $2",
        uid,
        config.CART_PAGE_LIMIT,
    )
    if not rows:
        return {"user_id": uid, "items": [], "total": 0.0}

    # Enrich every line item with one batched downstream call instead of a
    # per-item fan-out; the old unbounded asyncio.gather exhausted the product
    # connection pool under load and cascaded 5xxs up to the gateway.
    ids = ",".join(str(r["product_id"]) for r in rows)
    products = await downstream.get_json(
        "product", f"{config.PRODUCT_URL}/products/batch", params={"ids": ids}
    )

    items = []
    for r in rows:
        # JSON object keys are strings once serialised over HTTP.
        product = products.get(str(r["product_id"]))
        if product is None:
            # Product no longer exists (or was not returned); skip the line
            # rather than failing the whole cart.
            continue
        qty = r["qty"]
        items.append(
            {
                "product_id": r["product_id"],
                "qty": qty,
                "name": product["name"],
                "price": product["price"],
                "line_total": round(product["price"] * qty, 2),
            }
        )
    total = round(sum(i["line_total"] for i in items), 2)
    return {"user_id": uid, "items": items, "total": total}


@app.post("/carts/{uid}/items")
async def add_item(uid: int, payload: dict = Body(...)) -> dict:
    product_id = int(payload["product_id"])
    qty = int(payload.get("qty", 1))
    try:
        await db.execute(
            "cart_add_item",
            "INSERT INTO cart_items (user_id, product_id, qty) VALUES ($1, $2, $3)",
            uid,
            product_id,
            qty,
        )
    except asyncpg.ForeignKeyViolationError as exc:
        # cart_items now has FKs onto products/users, so an unknown id is a bad
        # request rather than the dangling row it used to insert silently.
        raise HTTPException(
            status_code=404, detail="product or user not found"
        ) from exc
    return {"user_id": uid, "product_id": product_id, "qty": qty, "status": "added"}


@app.delete("/carts/{uid}/items/{product_id}")
async def remove_item(uid: int, product_id: int) -> dict:
    """Remove a product from a user's cart.

    Nothing stops add_item from inserting the same product twice, so a product
    can hold several rows in one cart; this clears all of them and reports how
    many lines went away.
    """
    rows = await db.fetch(
        "cart_remove_item",
        "DELETE FROM cart_items WHERE user_id = $1 AND product_id = $2 RETURNING id",
        uid,
        product_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="item not in cart")
    return {
        "user_id": uid,
        "product_id": product_id,
        "removed": len(rows),
        "status": "removed",
    }
