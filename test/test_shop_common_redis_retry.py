from __future__ import annotations

import unittest

import fluxi_sdk_test_support  # noqa: F401

from redis.asyncio.sentinel import MasterNotFoundError
from redis.exceptions import ResponseError

from shop_common.redis import run_with_failover_retry


class _PoolStub:
    def __init__(self) -> None:
        self.disconnect_calls = 0

    async def disconnect(self, inuse_connections: bool = False) -> None:
        self.disconnect_calls += 1


class _RedisStub:
    def __init__(self) -> None:
        self.connection_pool = _PoolStub()


class TestShopCommonRedisRetry(unittest.IsolatedAsyncioTestCase):
    async def test_retries_master_failover_discovery_errors(self) -> None:
        redis = _RedisStub()
        attempts = {"count": 0}

        async def operation() -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise MasterNotFoundError("No master found for 'order-master'")
            return "ok"

        result = await run_with_failover_retry(
            redis,  # type: ignore[arg-type]
            operation,
            operation_name="test.master_failover",
        )

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["count"], 2)
        self.assertEqual(redis.connection_pool.disconnect_calls, 1)

    async def test_retries_loading_response_errors(self) -> None:
        redis = _RedisStub()
        attempts = {"count": 0}

        async def operation() -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ResponseError("LOADING Redis is loading the dataset in memory")
            return "ok"

        result = await run_with_failover_retry(
            redis,  # type: ignore[arg-type]
            operation,
            operation_name="test.loading",
        )

        self.assertEqual(result, "ok")
        self.assertEqual(attempts["count"], 2)
        self.assertEqual(redis.connection_pool.disconnect_calls, 1)
