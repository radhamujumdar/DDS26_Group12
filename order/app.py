import logging
import os
import random
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from msgspec import msgpack
from redis.asyncio import Redis

from clients.payment_client import PaymentClient
from clients.stock_client import StockClient
from coordinator.two_pc import TwoPCCoordinator
from logging_utils import log_event
from models import OrderValue
from repository.order_repo import OrderRepository
from repository.tx_repo import TxRepository


GATEWAY_URL = os.environ["GATEWAY_URL"]
logger = logging.getLogger("order-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Redis(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        password=os.environ["REDIS_PASSWORD"],
        db=int(os.environ["REDIS_DB"]),
    )
    http_client = httpx.AsyncClient()
    stock_client = StockClient(http_client, GATEWAY_URL)
    payment_client = PaymentClient(http_client, GATEWAY_URL)
    tx_repo = TxRepository(db)

    app.state.order_repo = OrderRepository(db)
    app.state.stock_client = stock_client
    app.state.coordinator = TwoPCCoordinator(
        stock_client=stock_client,
        payment_client=payment_client,
        tx_repo=tx_repo,
        order_repo=app.state.order_repo,
        logger=logger,
    )
    try:
        log_event(logger, "startup_recovery_begin", service="order-service")
        await app.state.coordinator.recover_active_transactions()
        log_event(logger, "startup_recovery_complete", service="order-service")
    except HTTPException as exc:
        log_event(
            logger,
            "startup_recovery_failed",
            level="warning",
            service="order-service",
            detail=str(exc.detail),
        )

    yield

    await http_client.aclose()
    await db.aclose()


app = FastAPI(title="order-service", lifespan=lifespan)


@app.post("/create/{user_id}")
async def create_order(user_id: str):
    key = await app.state.order_repo.create_order(user_id)
    return {"order_id": key}


@app.post("/batch_init/{n}/{n_items}/{n_users}/{item_price}")
async def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        return OrderValue(
            paid=False,
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
        )

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry()) for i in range(n)}
    await app.state.order_repo.batch_set_orders(kv_pairs)
    return {"msg": "Batch init for orders successful"}


@app.get("/find/{order_id}")
async def find_order(order_id: str):
    order_entry: OrderValue = await app.state.order_repo.get_order(order_id)
    return {
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
    }


@app.post("/addItem/{order_id}/{item_id}/{quantity}")
async def add_item(order_id: str, item_id: str, quantity: int):
    if int(quantity) <= 0:
        raise HTTPException(status_code=400, detail="Quantity must be greater than zero")

    order_entry: OrderValue = await app.state.order_repo.get_order(order_id)
    if order_entry.paid:
        raise HTTPException(status_code=400, detail=f"Order: {order_id} is already paid")

    item_reply = await app.state.stock_client.find_item(item_id)
    if item_reply.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} does not exist!")

    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    await app.state.order_repo.save_order(order_id, order_entry)
    return PlainTextResponse(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status_code=200,
    )


@app.post("/checkout/{order_id}")
async def checkout(order_id: str):
    order_entry: OrderValue = await app.state.order_repo.get_order(order_id)
    await app.state.coordinator.checkout(order_id, order_entry)
    return PlainTextResponse("Checkout successful", status_code=200)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
