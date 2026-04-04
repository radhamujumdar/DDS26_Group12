from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request
from fluxi_engine.observability import trace_logging_enabled
from redis.asyncio import Redis

from shop_common.config import RedisSettings
from shop_common.redis import (
    close_redis_client,
    create_redis_client,
    wait_until_redis_ready,
)

from .repositories.payment_repository import PaymentRepository
from .services.payment_service import PaymentService


@dataclass(slots=True)
class PaymentContainer:
    redis: Redis
    repository: PaymentRepository
    payment_service: PaymentService


async def build_payment_container() -> PaymentContainer:
    redis = create_redis_client(RedisSettings.from_env())
    await wait_until_redis_ready(
        redis,
        operation_name="payment.redis.ready",
        trace_logging_enabled=trace_logging_enabled(),
        timeout_seconds=30.0,
    )
    repository = PaymentRepository(redis)
    service = PaymentService(repository)
    return PaymentContainer(
        redis=redis,
        repository=repository,
        payment_service=service,
    )


async def close_payment_container(container: PaymentContainer) -> None:
    await close_redis_client(container.redis)


def get_payment_service(request: Request) -> PaymentService:
    return request.app.state.container.payment_service
