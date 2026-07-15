"""gateway — edge service and public entry point.

Fans out to product, cart and recommendation and aggregates their responses.
Kept deliberately thin: it does no data access of its own.
"""
import asyncio
import time
from typing import Any

from fastapi import Body, HTTPException
from fastapi.responses import Response

from common import config
from common.downstream import DownstreamClient
from common.instrumentation import create_app

downstream = DownstreamClient()

# The featured list is user-independent (same limit=8 query for everyone), so it
# is cached in-process for a short TTL and shared across all /home requests. A
# lock makes the refresh single-flight: only one request re-fetches on expiry
# while the rest await it, instead of every concurrent miss hitting product.
_FEATURED_TTL_S = 15.0
_featured_cache: tuple[float, Any] | None = None
_featured_lock = asyncio.Lock()


async def _get_featured() -> Any:
    global _featured_cache
    now = time.monotonic()
    cached = _featured_cache
    if cached is not None and now - cached[0] < _FEATURED_TTL_S:
        return cached[1]
    async with _featured_lock:
        # Re-check: another request may have refreshed while we waited.
        cached = _featured_cache
        if cached is not None and time.monotonic() - cached[0] < _FEATURED_TTL_S:
            return cached[1]
        featured = await downstream.get_json(
            "product", f"{config.PRODUCT_URL}/products", params={"limit": 8}
        )
        _featured_cache = (time.monotonic(), featured)
        return featured


async def _startup() -> None:
    await downstream.connect()


async def _shutdown() -> None:
    await downstream.disconnect()


app = create_app("gateway", startup=_startup, shutdown=_shutdown)


@app.get("/home/{uid}")
async def home(uid: int) -> dict:
    """Personalised home view aggregating three services."""
    try:
        cart, recs = await asyncio.gather(
            downstream.get_json("cart", f"{config.CART_URL}/carts/{uid}"),
            downstream.get_json(
                "recommendation", f"{config.RECOMMENDATION_URL}/recommendations/{uid}"
            ),
        )
        featured = await _get_featured()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="home upstream failed") from exc
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


@app.post("/products", status_code=201)
async def create_product(payload: dict = Body(...)) -> dict:
    try:
        return await downstream.post_json(
            "product", f"{config.PRODUCT_URL}/products", json=payload
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="product upstream failed") from exc


@app.post("/carts/{uid}/items")
async def add_to_cart(uid: int, payload: dict = Body(...)) -> dict:
    try:
        return await downstream.post_json(
            "cart", f"{config.CART_URL}/carts/{uid}/items", json=payload
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail="cart upstream failed") from exc
