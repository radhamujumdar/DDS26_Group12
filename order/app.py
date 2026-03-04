import logging
import os
import random

import redis
import requests
import uvicorn

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from msgspec import msgpack

from clients.payment_client import PaymentClient
from clients.stock_client import StockClient
from coordinator.two_pc import TwoPCCoordinator
from models import OrderValue
from repository.order_repo import OrderRepository
from repository.tx_repo import TxRepository


GATEWAY_URL = os.environ["GATEWAY_URL"]

app = FastAPI(title="order-service")
logger = logging.getLogger("order-service")

db: redis.Redis = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)
http_session = requests.Session()

order_repo = OrderRepository(db)
tx_repo = TxRepository(db)
stock_client = StockClient(http_session, GATEWAY_URL)
payment_client = PaymentClient(http_session, GATEWAY_URL)
coordinator = TwoPCCoordinator(
    stock_client=stock_client,
    payment_client=payment_client,
    tx_repo=tx_repo,
    logger=logger,
)


@app.on_event("shutdown")
def close_connections():
    db.close()
    http_session.close()


@app.post("/create/{user_id}")
def create_order(user_id: str):
    key = order_repo.create_order(user_id)
    return {"order_id": key}


@app.post("/batch_init/{n}/{n_items}/{n_users}/{item_price}")
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
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
    order_repo.batch_set_orders(kv_pairs)
    return {"msg": "Batch init for orders successful"}


@app.get("/find/{order_id}")
def find_order(order_id: str):
    order_entry: OrderValue = order_repo.get_order(order_id)
    return {
        "order_id": order_id,
        "paid": order_entry.paid,
        "items": order_entry.items,
        "user_id": order_entry.user_id,
        "total_cost": order_entry.total_cost,
    }


@app.post("/addItem/{order_id}/{item_id}/{quantity}")
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = order_repo.get_order(order_id)
    item_reply = stock_client.find_item(item_id)
    if item_reply.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} does not exist!")

    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    order_repo.save_order(order_id, order_entry)
    return PlainTextResponse(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status_code=200,
    )


@app.post("/checkout/{order_id}")
def checkout(order_id: str):
    order_entry: OrderValue = order_repo.get_order(order_id)
    coordinator.checkout(order_id, order_entry)
    order_entry.paid = True
    order_repo.save_order(order_id, order_entry)
    return PlainTextResponse("Checkout successful", status_code=200)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
