from __future__ import annotations

import os
import unittest

import fluxi_sdk_test_support  # noqa: F401

from shop_common.config import RedisSettings


class TestShopCommonRedisSettings(unittest.TestCase):
    def setUp(self) -> None:
        self._previous = dict(os.environ)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._previous)

    def test_direct_mode_requires_host_port_and_db(self) -> None:
        os.environ["REDIS_MODE"] = "direct"
        os.environ["REDIS_HOST"] = "order-db"
        os.environ["REDIS_PORT"] = "6379"
        os.environ["REDIS_DB"] = "0"
        os.environ["REDIS_PASSWORD"] = "redis"

        settings = RedisSettings.from_env()

        self.assertEqual(settings.redis_mode, "direct")
        self.assertEqual(settings.host, "order-db")
        self.assertEqual(settings.port, 6379)
        self.assertEqual(settings.db, 0)
        self.assertEqual(settings.password, "redis")

    def test_sentinel_mode_can_omit_direct_host_values(self) -> None:
        os.environ["REDIS_MODE"] = "sentinel"
        os.environ["REDIS_SENTINEL_ENDPOINTS"] = (
            "order-sentinel-1:26379,order-sentinel-2:26379,order-sentinel-3:26379"
        )
        os.environ["REDIS_SENTINEL_SERVICE_NAME"] = "order-master"
        os.environ["REDIS_SENTINEL_MIN_OTHER_SENTINELS"] = "1"
        os.environ["REDIS_PASSWORD"] = "redis"
        os.environ["REDIS_DB"] = "0"

        settings = RedisSettings.from_env()

        self.assertEqual(settings.redis_mode, "sentinel")
        self.assertEqual(settings.sentinel_service_name, "order-master")
        self.assertEqual(
            settings.sentinel_endpoints,
            "order-sentinel-1:26379,order-sentinel-2:26379,order-sentinel-3:26379",
        )
        self.assertEqual(settings.sentinel_min_other_sentinels, 1)
        self.assertEqual(settings.password, "redis")
        self.assertEqual(settings.db, 0)

    def test_failover_retry_timeout_can_be_overridden_from_env(self) -> None:
        os.environ["REDIS_MODE"] = "sentinel"
        os.environ["REDIS_SENTINEL_ENDPOINTS"] = (
            "payment-sentinel-1:26379,payment-sentinel-2:26379,payment-sentinel-3:26379"
        )
        os.environ["REDIS_SENTINEL_SERVICE_NAME"] = "payment-master"
        os.environ["REDIS_DB"] = "0"
        os.environ["REDIS_FAILOVER_RETRY_TIMEOUT_SECONDS"] = "30.0"

        settings = RedisSettings.from_env()

        self.assertEqual(settings.failover_retry_timeout_seconds, 30.0)
