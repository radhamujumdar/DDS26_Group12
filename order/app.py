import logging
from contextlib import asynccontextmanager

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis

from api import router
from clients.payment_client import PaymentClient
from clients.saga_bus import SagaCommandBus
from clients.stock_client import StockClient
from clients.two_pc_bus import TwoPCCommandBus
from config import OrderConfig
from coordinator.saga import SagaCoordinator
from coordinator.two_pc import TwoPCCoordinator
from logging_utils import log_event
from models import TxMode
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
    two_pc_bus: TwoPCCommandBus | None = None
    # 2pc message queue change: 2PC now shares the existing MQ broker/settings so
    # both transaction modes use Redis Streams transport without new infra.
    if config.tx_mode in (TxMode.TWO_PC.value, TxMode.SAGA.value):
        saga_broker_db = create_redis_client(
            host=config.saga_mq_redis_host,
            port=config.saga_mq_redis_port,
            password=config.saga_mq_redis_password,
            db=config.saga_mq_redis_db,
            sentinel_hosts=config.saga_mq_sentinel_hosts,
            master_name=config.saga_mq_master_name,
        )
        if config.tx_mode == TxMode.TWO_PC.value:
            two_pc_bus = TwoPCCommandBus(
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
            await two_pc_bus.start()
            await two_pc_bus.recover_stale_pending(stale_after_ms=config.saga_mq_pending_stale_after_ms)
        else:
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
    # 2pc message queue change: inject the 2PC bus into the normal clients so the
    # coordinator can keep using the same client APIs regardless of transport.
    stock_client = StockClient(http_client, config.stock_service_url, saga_bus=saga_bus, two_pc_bus=two_pc_bus)
    payment_client = PaymentClient(
        http_client,
        config.payment_service_url,
        saga_bus=saga_bus,
        two_pc_bus=two_pc_bus,
    )
    tx_repo = TxRepository(db)
    saga_repo = SagaTxRepository(db)

    app.state.config = config
    app.state.order_repo = OrderRepository(db)
    app.state.stock_client = stock_client
    app.state.tx_mode = config.tx_mode
    app.state.saga_bus = saga_bus
    app.state.two_pc_bus = two_pc_bus
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
    if two_pc_bus is not None:
        await two_pc_bus.stop()
    if saga_broker_db is not None:
        await saga_broker_db.aclose()
    await db.aclose()


app = FastAPI(title="order-service", lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": "ok"}


app.include_router(router)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
