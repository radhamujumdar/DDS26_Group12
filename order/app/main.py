from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fluxi_engine.observability import configure_logging
from shop_common.redis import wait_until_redis_ready

from .controllers.orders import router
from .dependencies import build_order_api_container, close_order_api_container


configure_logging("order-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = await build_order_api_container()
    app.state.container = container
    try:
        yield
    finally:
        await close_order_api_container(container)


app = FastAPI(title="order-service", lifespan=lifespan)
app.include_router(router)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    container = app.state.container
    try:
        await wait_until_redis_ready(
            container.redis,
            operation_name="order.redis.readyz",
            timeout_seconds=2.0,
        )
        response = await container.fluxi_ready_client.get("/readyz")
        response.raise_for_status()
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Order service dependencies are not ready.",
        ) from exc
    return {"status": "ready"}
