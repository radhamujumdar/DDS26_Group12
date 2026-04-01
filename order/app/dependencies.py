from __future__ import annotations

from dataclasses import dataclass

import httpx
from fastapi import Request
from redis.asyncio import Redis

from fluxi_sdk.client import EngineConnectionConfig, WorkflowClient
from shop_common.config import GatewaySettings, RedisSettings
from shop_common.redis import close_redis_client, create_redis_client

from .clients.stock_client import StockGatewayClient
from .repositories.order_repository import OrderRepository
from .services.checkout_service import CheckoutService
from .services.order_service import OrderService


@dataclass(slots=True)
class OrderApiContainer:
    redis: Redis
    gateway_client: httpx.AsyncClient
    repository: OrderRepository
    stock_client: StockGatewayClient
    order_service: OrderService
    checkout_service: CheckoutService


@dataclass(slots=True)
class OrderWorkerContainer:
    redis: Redis
    repository: OrderRepository
    order_service: OrderService


async def build_order_api_container() -> OrderApiContainer:
    redis = create_redis_client(RedisSettings.from_env())
    gateway_client = httpx.AsyncClient(
        base_url=GatewaySettings.from_env().gateway_url,
        timeout=5.0,
    )
    repository = OrderRepository(redis)
    stock_client = StockGatewayClient(gateway_client)
    order_service = OrderService(repository, stock_client)
    checkout_service = CheckoutService(
        WorkflowClient.connect(engine=EngineConnectionConfig.from_env())
    )
    return OrderApiContainer(
        redis=redis,
        gateway_client=gateway_client,
        repository=repository,
        stock_client=stock_client,
        order_service=order_service,
        checkout_service=checkout_service,
    )


async def close_order_api_container(container: OrderApiContainer) -> None:
    await container.checkout_service.aclose()
    await container.gateway_client.aclose()
    await close_redis_client(container.redis)


async def build_order_worker_container() -> OrderWorkerContainer:
    redis = create_redis_client(RedisSettings.from_env())
    repository = OrderRepository(redis)
    order_service = OrderService(repository)
    return OrderWorkerContainer(
        redis=redis,
        repository=repository,
        order_service=order_service,
    )


async def close_order_worker_container(container: OrderWorkerContainer) -> None:
    await close_redis_client(container.redis)


def get_order_api_container(request: Request) -> OrderApiContainer:
    return request.app.state.container


def get_order_service(request: Request) -> OrderService:
    return get_order_api_container(request).order_service


def get_checkout_service(request: Request) -> CheckoutService:
    return get_order_api_container(request).checkout_service
