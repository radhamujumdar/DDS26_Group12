from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request
from redis.asyncio import Redis

from shop_common.config import RedisSettings
from shop_common.redis import close_redis_client, create_redis_client

from .repositories.payment_repository import PaymentRepository
from .services.payment_service import PaymentService


@dataclass(slots=True)
class PaymentContainer:
    redis: Redis
    repository: PaymentRepository
    payment_service: PaymentService


async def build_payment_container() -> PaymentContainer:
    redis = create_redis_client(RedisSettings.from_env())
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
