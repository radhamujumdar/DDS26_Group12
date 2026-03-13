import logging
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
from config import OrderConfig
from coordinator.saga import SagaCoordinator
from coordinator.two_pc import TwoPCCoordinator
from logging_utils import log_event
from models import OrderValue, TxMode
from redis_utils import create_redis_client
from repository.order_repo import OrderRepository
from repository.saga_repo import SagaTxRepository
from repository.tx_repo import TxRepository

logger = logging.getLogger("order-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = OrderConfig.from_env()
    if config.tx_mode not in (TxMode.TWO_PC.value, TxMode.SAGA.value):
        raise RuntimeError(f"Unsupported TX_MODE={config.tx_mode}. Expected one of: 2pc, saga")

    db = create_redis_client(
        host=config.redis_host,
        port=config.redis_port,
        password=config.redis_password,
        db=config.redis_db,
        sentinel_hosts=config.redis_sentinel_hosts,
        master_name=config.redis_master_name,
    )
    saga_broker_db: Redis | None = None
    saga_bus: SagaCommandBus | None = None
    if config.tx_mode == TxMode.SAGA.value:
        saga_broker_db = create_redis_client(
            host=config.saga_mq_redis_host,
            port=config.saga_mq_redis_port,
            password=config.saga_mq_redis_password,
            db=config.saga_mq_redis_db,
            sentinel_hosts=config.saga_mq_sentinel_hosts,
            master_name=config.saga_mq_master_name,
        )
        saga_bus = SagaCommandBus(
            db=saga_broker_db,
            logger=logger,
            stream_partitions=config.saga_mq_stream_partitions,
            response_timeout_ms=config.saga_mq_response_timeout_ms,
            command_stream_maxlen=config.saga_mq_command_stream_maxlen,
            result_stream_maxlen=config.saga_mq_result_stream_maxlen,
            pending_ttl_seconds=config.saga_mq_pending_ttl_seconds,
            poll_interval_seconds=config.saga_mq_poll_interval_seconds,
            enable_dispatcher=config.enable_order_dispatcher,
            dispatcher_block_ms=config.saga_mq_dispatch_block_ms,
            dispatch_lease_ttl_seconds=config.saga_mq_dispatch_lease_ttl_seconds,
            dispatch_renew_interval_seconds=config.saga_mq_dispatch_renew_interval_seconds,
            owner_id=config.saga_mq_owner_id,
        )
        await saga_bus.start()
        await saga_bus.recover_stale_pending(stale_after_ms=config.saga_mq_pending_stale_after_ms)

    http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=2000, max_keepalive_connections=1000),
        timeout=httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=30.0),
    )
    stock_client = StockClient(http_client, config.stock_service_url, saga_bus=saga_bus)
    payment_client = PaymentClient(http_client, config.payment_service_url, saga_bus=saga_bus)
    tx_repo = TxRepository(db)
    saga_repo = SagaTxRepository(db)

    app.state.config = config
    app.state.order_repo = OrderRepository(db)
    app.state.stock_client = stock_client
    app.state.tx_mode = config.tx_mode
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
        if config.tx_mode == TxMode.SAGA.value:
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


@app.get("/health")
async def health_check():
    return {"status": "ok"}


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
