from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .controllers.payment import router
from .dependencies import build_payment_container, close_payment_container


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
