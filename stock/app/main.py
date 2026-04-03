from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fluxi_engine.observability import configure_logging

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
