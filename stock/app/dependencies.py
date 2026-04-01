from __future__ import annotations

from dataclasses import dataclass

from fastapi import Request
from redis.asyncio import Redis

from shop_common.config import RedisSettings
from shop_common.redis import close_redis_client, create_redis_client

from .repositories.stock_repository import StockRepository
from .services.stock_service import StockService


@dataclass(slots=True)
class StockContainer:
    redis: Redis
    repository: StockRepository
    stock_service: StockService


async def build_stock_container() -> StockContainer:
    redis = create_redis_client(RedisSettings.from_env())
    repository = StockRepository(redis)
    service = StockService(repository)
    return StockContainer(redis=redis, repository=repository, stock_service=service)


async def close_stock_container(container: StockContainer) -> None:
    await close_redis_client(container.redis)


def get_stock_service(request: Request) -> StockService:
    return request.app.state.container.stock_service
