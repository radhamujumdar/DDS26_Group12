import asyncio
import logging
from contextlib import asynccontextmanager, suppress

import httpx
import uvicorn
from fastapi import FastAPI
from redis.asyncio import Redis

from api import router
from config import PaymentConfig
from logging_utils import log_event
from repository.payment_repo import PaymentRepository
from services import PaymentRecoveryService, PaymentService


logger = logging.getLogger("payment-service")


@asynccontextmanager
async def lifespan(the_app: FastAPI):
    config = PaymentConfig.from_env()
    db = Redis(
        host=config.redis_host,
        port=config.redis_port,
        password=config.redis_password,
        db=config.redis_db,
    )
    gateway_client = httpx.AsyncClient(timeout=2.0)
    payment_repo = PaymentRepository(db)
    payment_service = PaymentService(payment_repo, logger)
    recovery_service = PaymentRecoveryService(
        repo=payment_repo,
        payment_service=payment_service,
        gateway_client=gateway_client,
        gateway_url=config.gateway_url,
        logger=logger,
    )

    the_app.state.config = config
    the_app.state.db = db
    the_app.state.payment_repository = payment_repo
    the_app.state.payment_service = payment_service
    the_app.state.recovery_service = recovery_service
    the_app.state.gateway_client = gateway_client

    log_event(logger, "startup_begin", service="payment-service")
    the_app.state.recovery_task = asyncio.create_task(
        recovery_service.run_loop(
            startup_delay_seconds=config.recovery_startup_delay_seconds,
            interval_seconds=config.recovery_interval_seconds,
        )
    )
    log_event(logger, "startup_complete", service="payment-service")

    yield

    log_event(logger, "shutdown_begin", service="payment-service")
    the_app.state.recovery_task.cancel()
    with suppress(asyncio.CancelledError):
        await the_app.state.recovery_task
    await gateway_client.aclose()
    await db.aclose()
    log_event(logger, "shutdown_complete", service="payment-service")


app = FastAPI(title="payment-service", lifespan=lifespan)
app.include_router(router)


def get_db() -> Redis:
    return app.state.db


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logger.handlers = gunicorn_logger.handlers
    logger.setLevel(gunicorn_logger.level)
