from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fluxi_engine.observability import configure_logging
from shop_common.redis import wait_until_redis_ready

from .controllers.payment import router
from .dependencies import build_payment_container, close_payment_container


configure_logging("payment-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = await build_payment_container()
    app.state.container = container
    try:
        yield
    finally:
        await close_payment_container(container)


app = FastAPI(title="payment-service", lifespan=lifespan)
app.include_router(router)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    try:
        await wait_until_redis_ready(
            app.state.container.redis,
            operation_name="payment.redis.readyz",
            timeout_seconds=2.0,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Payment service dependencies are not ready.",
        ) from exc
    return {"status": "ready"}
