"""gateway — edge service and public entry point.

Fans out to product, cart and recommendation and aggregates their responses.
Kept deliberately thin: it does no data access of its own.
"""
import asyncio

from fastapi import Body, HTTPException
from fastapi.responses import Response

from common import config
from common.downstream import DownstreamClient
from common.instrumentation import create_app

downstream = DownstreamClient()


async def _startup() -> None:
    await downstream.connect()


async def _shutdown() -> None:
    await downstream.disconnect()


app = create_app("gateway", startup=_startup, shutdown=_shutdown)


@app.get("/home/{uid}")
async def home(uid: int) -> dict:
    """Personalised home view aggregating three services."""
    cart, recs = await asyncio.gather(
        downstream.get_json("cart", f"{config.CART_URL}/carts/{uid}"),
        downstream.get_json(
            "recommendation", f"{config.RECOMMENDATION_URL}/recommendations/{uid}"
        ),
    )
    featured = await downstream.get_json(
        "product", f"{config.PRODUCT_URL}/products", params={"limit": 8}
    )
    return {
        "user_id": uid,
        "cart": cart,
        "recommendations": recs["items"],
        "featured": featured,
    }


@app.get("/products/{pid}")
async def product(pid: int) -> Response:
    # Pure pass-through: stream the upstream bytes straight to the client
    # instead of parsing JSON and re-serialising it.
    try:
        content, media_type = await downstream.get_raw(
            "product", f"{config.PRODUCT_URL}/products/{pid}"
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="product upstream failed") from exc
    return Response(content=content, media_type=media_type)


@app.post("/carts/{uid}/items")
async def add_to_cart(uid: int, payload: dict = Body(...)) -> dict:
    return await downstream.post_json(
        "cart", f"{config.CART_URL}/carts/{uid}/items", json=payload
    )
