import asyncio
import logging
from contextlib import asynccontextmanager, suppress

import httpx
import uvicorn
from fastapi import FastAPI
from redis.asyncio import Redis

from api import router
from config import StockConfig
from logging_utils import log_event
from repository.stock_repo import StockRepository
from services import StockRecoveryService, StockService


logger = logging.getLogger("stock-service")


@asynccontextmanager
async def lifespan(the_app: FastAPI):
    config = StockConfig.from_env()
    db = Redis(
        host=config.redis_host,
        port=config.redis_port,
        password=config.redis_password,
        db=config.redis_db,
    )
    gateway_client = httpx.AsyncClient(timeout=2.0)
    stock_repo = StockRepository(db)
    stock_service = StockService(stock_repo, logger)
    recovery_service = StockRecoveryService(
        repo=stock_repo,
        stock_service=stock_service,
        gateway_client=gateway_client,
        gateway_url=config.gateway_url,
        logger=logger,
    )
    saga_broker_db: Redis | None = None
    saga_worker_task: asyncio.Task | None = None
    saga_worker = None
    if config.saga_mq_enabled:
        saga_broker_db = Redis(
            host=config.saga_mq_redis_host,
            port=config.saga_mq_redis_port,
            password=config.saga_mq_redis_password,
            db=config.saga_mq_redis_db,
        )
        from services import StockSagaMqWorkerService

        saga_worker = StockSagaMqWorkerService(
            db=saga_broker_db,
            stock_service=stock_service,
            logger=logger,
            stream_partitions=config.saga_mq_stream_partitions,
            consumer_group=config.saga_mq_consumer_group,
            block_ms=config.saga_mq_block_ms,
            batch_size=config.saga_mq_batch_size,
            command_stream_maxlen=config.saga_mq_command_stream_maxlen,
            result_stream_maxlen=config.saga_mq_result_stream_maxlen,
        )

    the_app.state.config = config
    the_app.state.db = db
    the_app.state.stock_repository = stock_repo
    the_app.state.stock_service = stock_service
    the_app.state.recovery_service = recovery_service
    the_app.state.gateway_client = gateway_client
    the_app.state.saga_broker_db = saga_broker_db
    the_app.state.saga_worker = saga_worker

    log_event(logger, "startup_begin", service="stock-service")
    the_app.state.recovery_task = asyncio.create_task(
        recovery_service.run_loop(
            startup_delay_seconds=config.recovery_startup_delay_seconds,
            interval_seconds=config.recovery_interval_seconds,
        )
    )
    if saga_worker is not None:
        saga_worker_task = asyncio.create_task(saga_worker.run_loop())
    the_app.state.saga_worker_task = saga_worker_task
    log_event(logger, "startup_complete", service="stock-service")

    yield

    log_event(logger, "shutdown_begin", service="stock-service")
    the_app.state.recovery_task.cancel()
    with suppress(asyncio.CancelledError):
        await the_app.state.recovery_task
    if the_app.state.saga_worker_task is not None:
        the_app.state.saga_worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await the_app.state.saga_worker_task
    await gateway_client.aclose()
    if saga_broker_db is not None:
        await saga_broker_db.aclose()
    await db.aclose()
    log_event(logger, "shutdown_complete", service="stock-service")


app = FastAPI(title="stock-service", lifespan=lifespan)
app.include_router(router)


def get_db() -> Redis:
    return app.state.db


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
