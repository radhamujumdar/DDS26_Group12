from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .controllers.orders import router
from .dependencies import build_order_api_container, close_order_api_container


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
