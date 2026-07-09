"""cart — read/write service; calls product to enrich line items."""
from fastapi import Body

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
        "SELECT product_id, qty FROM cart_items WHERE user_id = $1 ORDER BY added_at LIMIT 100",
        uid,
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
    await db.execute(
        "cart_add_item",
        "INSERT INTO cart_items (user_id, product_id, qty) VALUES ($1, $2, $3)",
        uid,
        product_id,
        qty,
    )
    return {"user_id": uid, "product_id": product_id, "qty": qty, "status": "added"}
