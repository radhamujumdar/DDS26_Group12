from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time

import httpx
from fastapi import Request
from redis.asyncio import Redis

from fluxi_sdk.client import EngineConnectionConfig, WorkflowClient
from fluxi_engine.observability import trace_logging_enabled
from shop_common.config import GatewaySettings, RedisSettings
from shop_common.redis import (
    close_redis_client,
    create_redis_client,
    wait_until_redis_ready,
)

from .clients.stock_client import StockGatewayClient
from .repositories.order_repository import OrderRepository
from .services.checkout_service import CheckoutService
from .services.order_service import OrderService


logger = logging.getLogger(__name__)
TRACE_LOGGING_ENABLED = trace_logging_enabled()


@dataclass(slots=True)
class OrderApiContainer:
    redis: Redis
    gateway_client: httpx.AsyncClient
    fluxi_ready_client: httpx.AsyncClient
    repository: OrderRepository
    stock_client: StockGatewayClient
    order_service: OrderService
    checkout_service: CheckoutService


@dataclass(slots=True)
class OrderWorkerContainer:
    redis: Redis
    repository: OrderRepository
    order_service: OrderService


async def _wait_until_http_ready(
    client: httpx.AsyncClient,
    *,
    path: str,
    operation_name: str,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    delay_seconds = 0.1
    attempt_no = 1

    while True:
        try:
            response = await client.get(path)
            if response.status_code == 200:
                return
        except httpx.HTTPError as exc:
            if TRACE_LOGGING_ENABLED:
                logger.warning(
                    "%s transient upstream readiness error attempt=%d message=%s",
                    operation_name,
                    attempt_no,
                    exc,
                )
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting for {operation_name}.")
        await asyncio.sleep(delay_seconds)
        delay_seconds = min(delay_seconds * 2, 0.75)
        attempt_no += 1


async def build_order_api_container() -> OrderApiContainer:
    redis = create_redis_client(RedisSettings.from_env())
    gateway_client = httpx.AsyncClient(
        base_url=GatewaySettings.from_env().gateway_url,
        timeout=5.0,
    )
    engine_config = EngineConnectionConfig.from_env()
    fluxi_ready_client = httpx.AsyncClient(
        base_url=engine_config.server_url,
        timeout=5.0,
    )
    try:
        await wait_until_redis_ready(
            redis,
            operation_name="order.redis.ready",
            logger=logger,
            trace_logging_enabled=TRACE_LOGGING_ENABLED,
            timeout_seconds=30.0,
        )
        await _wait_until_http_ready(
            fluxi_ready_client,
            path="/readyz",
            operation_name="fluxi-server.ready",
            timeout_seconds=30.0,
        )
        repository = OrderRepository(redis)
        stock_client = StockGatewayClient(gateway_client)
        order_service = OrderService(repository, stock_client)
        checkout_service = CheckoutService(
            WorkflowClient.connect(engine=engine_config),
            order_service,
        )
        return OrderApiContainer(
            redis=redis,
            gateway_client=gateway_client,
            fluxi_ready_client=fluxi_ready_client,
            repository=repository,
            stock_client=stock_client,
            order_service=order_service,
            checkout_service=checkout_service,
        )
    except BaseException:
        await fluxi_ready_client.aclose()
        await gateway_client.aclose()
        await close_redis_client(redis)
        raise


async def close_order_api_container(container: OrderApiContainer) -> None:
    await container.checkout_service.aclose()
    await container.fluxi_ready_client.aclose()
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
