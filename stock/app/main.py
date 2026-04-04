from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fluxi_engine.observability import configure_logging
from shop_common.redis import wait_until_redis_ready

from .controllers.stock import router
from .dependencies import build_stock_container, close_stock_container


configure_logging("stock-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = await build_stock_container()
    app.state.container = container
    try:
        yield
    finally:
        await close_stock_container(container)


app = FastAPI(title="stock-service", lifespan=lifespan)
app.include_router(router)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    try:
        await wait_until_redis_ready(
            app.state.container.redis,
            operation_name="stock.redis.readyz",
            timeout_seconds=2.0,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Stock service dependencies are not ready.",
        ) from exc
    return {"status": "ready"}
