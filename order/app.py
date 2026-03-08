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
from clients.saga_bus import SagaCommandBus
from clients.stock_client import StockClient
from coordinator.saga import SagaCoordinator
from coordinator.two_pc import TwoPCCoordinator
from logging_utils import log_event
from redis_utils import create_redis_client
from models import OrderValue, TxMode
from repository.order_repo import OrderRepository
from repository.saga_repo import SagaTxRepository
from repository.tx_repo import TxRepository


GATEWAY_URL = os.environ["GATEWAY_URL"]
TX_MODE = os.environ.get("TX_MODE", TxMode.TWO_PC.value).lower()
ENABLE_ORDER_DISPATCHER = os.environ.get("ENABLE_ORDER_DISPATCHER", "true").lower() in ("1", "true", "yes", "on")
logger = logging.getLogger("order-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if TX_MODE not in (TxMode.TWO_PC.value, TxMode.SAGA.value):
        raise RuntimeError(f"Unsupported TX_MODE={TX_MODE}. Expected one of: 2pc, saga")

    db = create_redis_client(
        host=os.environ["REDIS_HOST"],
        port=int(os.environ["REDIS_PORT"]),
        password=os.environ["REDIS_PASSWORD"],
        db=int(os.environ["REDIS_DB"]),
        sentinel_hosts=os.environ.get("REDIS_SENTINEL_HOSTS"),
        master_name=os.environ.get("REDIS_MASTER_NAME"),
    )
    saga_broker_db: Redis | None = None
    saga_bus: SagaCommandBus | None = None
    if TX_MODE == TxMode.SAGA.value:
        saga_broker_db = create_redis_client(
            host=os.environ.get("SAGA_MQ_REDIS_HOST", os.environ["REDIS_HOST"]),
            port=int(os.environ.get("SAGA_MQ_REDIS_PORT", os.environ["REDIS_PORT"])),
            password=os.environ.get("SAGA_MQ_REDIS_PASSWORD", os.environ["REDIS_PASSWORD"]),
            db=int(os.environ.get("SAGA_MQ_REDIS_DB", "0")),
            sentinel_hosts=os.environ.get("SAGA_MQ_SENTINEL_HOSTS"),
            master_name=os.environ.get("SAGA_MQ_MASTER_NAME"),
        )
        saga_bus = SagaCommandBus(
            db=saga_broker_db,
            logger=logger,
            stream_partitions=int(os.environ.get("SAGA_MQ_STREAM_PARTITIONS", "4")),
            response_timeout_ms=int(os.environ.get("SAGA_MQ_RESPONSE_TIMEOUT_MS", "3000")),
            command_stream_maxlen=int(os.environ.get("SAGA_MQ_COMMAND_STREAM_MAXLEN", "100000")),
            result_stream_maxlen=int(os.environ.get("SAGA_MQ_RESULT_STREAM_MAXLEN", "100000")),
            pending_ttl_seconds=int(os.environ.get("SAGA_MQ_PENDING_TTL_SECONDS", "3600")),
            poll_interval_seconds=float(os.environ.get("SAGA_MQ_POLL_INTERVAL_SECONDS", "0.05")),
            enable_dispatcher=ENABLE_ORDER_DISPATCHER,
            dispatcher_block_ms=int(os.environ.get("SAGA_MQ_DISPATCH_BLOCK_MS", "1000")),
            dispatch_lease_ttl_seconds=int(os.environ.get("SAGA_MQ_DISPATCH_LEASE_TTL_SECONDS", "10")),
            dispatch_renew_interval_seconds=float(
                os.environ.get("SAGA_MQ_DISPATCH_RENEW_INTERVAL_SECONDS", "2.0")
            ),
            owner_id=os.environ.get("SAGA_MQ_OWNER_ID"),
        )
        await saga_bus.start()
        await saga_bus.recover_stale_pending(
            stale_after_ms=int(os.environ.get("SAGA_MQ_PENDING_STALE_AFTER_MS", "3000"))
        )

    http_client = httpx.AsyncClient()
    stock_client = StockClient(http_client, GATEWAY_URL, saga_bus=saga_bus)
    payment_client = PaymentClient(http_client, GATEWAY_URL, saga_bus=saga_bus)
    tx_repo = TxRepository(db)
    saga_repo = SagaTxRepository(db)

    app.state.order_repo = OrderRepository(db)
    app.state.stock_client = stock_client
    app.state.tx_mode = TX_MODE
    app.state.saga_bus = saga_bus
    app.state.saga_broker_db = saga_broker_db
    app.state.coordinator = TwoPCCoordinator(
        stock_client=stock_client,
        payment_client=payment_client,
        tx_repo=tx_repo,
        order_repo=app.state.order_repo,
        logger=logger,
    )
    app.state.saga_coordinator = SagaCoordinator(
        stock_client=stock_client,
        payment_client=payment_client,
        saga_repo=saga_repo,
        order_repo=app.state.order_repo,
        logger=logger,
    )
    try:
        log_event(logger, "startup_recovery_begin", service="order-service")
        if app.state.tx_mode == TxMode.SAGA.value:
            await app.state.saga_coordinator.recover_active_transactions()
        else:
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
    if saga_bus is not None:
        await saga_bus.stop()
    if saga_broker_db is not None:
        await saga_broker_db.aclose()
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
    if app.state.tx_mode == TxMode.SAGA.value:
        await app.state.saga_coordinator.checkout(order_id, order_entry)
    else:
        await app.state.coordinator.checkout(order_id, order_entry)
    return PlainTextResponse("Checkout successful", status_code=200)


@app.get("/2pc/tx/{tx_id}")
async def get_tx_state(tx_id: str):
    tx = await app.state.coordinator.tx_repo.get(tx_id)
    if tx is None:
        raise HTTPException(status_code=400, detail=f"Transaction {tx_id} not found")
    return {"tx_id": tx.tx_id, "state": tx.state}


@app.get("/saga/tx/{tx_id}")
async def get_saga_tx_state(tx_id: str):
    tx = await app.state.saga_coordinator.saga_repo.get(tx_id)
    if tx is None:
        raise HTTPException(status_code=400, detail=f"Saga transaction {tx_id} not found")
    return {"tx_id": tx.tx_id, "state": tx.state}


@app.get("/saga/metrics")
async def get_saga_metrics():
    if app.state.saga_bus is None:
        raise HTTPException(status_code=400, detail="Saga mode is disabled")
    snapshot = await app.state.saga_bus.get_metrics_snapshot()
    active_ids = await app.state.saga_coordinator.saga_repo.list_active()
    snapshot["active_saga_transactions"] = len(active_ids)
    snapshot["tx_mode"] = app.state.tx_mode
    return snapshot


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
