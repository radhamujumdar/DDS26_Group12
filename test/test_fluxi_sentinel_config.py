import os
import unittest
from unittest import TestCase
from unittest.mock import patch

import fluxi_sdk_test_support  # noqa: F401

import fluxi_engine.config as engine_config
from fluxi_engine.config import FluxiSettings
from fluxi_sdk.client import EngineConnectionConfig


class TestFluxiSentinelConfig(TestCase):

    def test_fluxi_settings_accepts_sentinel_mode(self) -> None:
        settings = FluxiSettings(
            redis_mode="sentinel",
            redis_url="redis://:redis@redis-master:6379/0",
            sentinel_endpoints="sentinel-1:26379,sentinel-2:26379",
            sentinel_service_name="fluxi-master",
        )

        self.assertEqual(settings.redis_mode, "sentinel")
        self.assertEqual(
            settings.sentinel_endpoints,
            "sentinel-1:26379,sentinel-2:26379",
        )
        self.assertEqual(settings.sentinel_service_name, "fluxi-master")

    def test_fluxi_settings_rejects_invalid_sentinel_endpoints(self) -> None:
        with self.assertRaises(ValueError):
            FluxiSettings(
                redis_mode="sentinel",
                sentinel_endpoints="sentinel-1",
                sentinel_service_name="fluxi-master",
            )

    def test_fluxi_settings_from_env_reads_sentinel_fields(self) -> None:
        with patch.dict(
            os.environ,
            {
                "FLUXI_REDIS_MODE": "sentinel",
                "FLUXI_REDIS_URL": "redis://:redis@redis-master:6379/2",
                "FLUXI_SENTINEL_ENDPOINTS": "sentinel-1:26379,sentinel-2:26379",
                "FLUXI_SENTINEL_SERVICE_NAME": "fluxi-master",
                "FLUXI_SENTINEL_MIN_OTHER_SENTINELS": "2",
                "FLUXI_SENTINEL_USERNAME": "sentinel-user",
                "FLUXI_SENTINEL_PASSWORD": "sentinel-pass",
            },
            clear=False,
        ):
            settings = FluxiSettings.from_env()

        self.assertEqual(settings.redis_mode, "sentinel")
        self.assertEqual(settings.redis_url, "redis://:redis@redis-master:6379/2")
        self.assertEqual(settings.sentinel_endpoints, "sentinel-1:26379,sentinel-2:26379")
        self.assertEqual(settings.sentinel_service_name, "fluxi-master")
        self.assertEqual(settings.sentinel_min_other_sentinels, 2)
        self.assertEqual(settings.sentinel_username, "sentinel-user")
        self.assertEqual(settings.sentinel_password, "sentinel-pass")

    def test_engine_connection_config_from_env_reads_sentinel_fields(self) -> None:
        with patch.dict(
            os.environ,
            {
                "FLUXI_SERVER_URL": "http://fluxi-server:8001",
                "FLUXI_REDIS_MODE": "sentinel",
                "FLUXI_REDIS_URL": "redis://:redis@redis-master:6379/1",
                "FLUXI_SENTINEL_ENDPOINTS": "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379",
                "FLUXI_SENTINEL_SERVICE_NAME": "fluxi-master",
                "FLUXI_SENTINEL_MIN_OTHER_SENTINELS": "1",
            },
            clear=False,
        ):
            config = EngineConnectionConfig.from_env()

        self.assertEqual(config.server_url, "http://fluxi-server:8001")
        self.assertEqual(config.redis_mode, "sentinel")
        self.assertEqual(config.redis_url, "redis://:redis@redis-master:6379/1")
        self.assertEqual(
            config.sentinel_endpoints,
            "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379",
        )
        self.assertEqual(config.sentinel_service_name, "fluxi-master")
        self.assertEqual(config.sentinel_min_other_sentinels, 1)

    def test_engine_connection_config_rejects_empty_sentinel_service_name(self) -> None:
        with self.assertRaises(ValueError):
            EngineConnectionConfig(
                server_url="http://localhost:8001",
                redis_mode="sentinel",
                sentinel_endpoints="sentinel-1:26379",
                sentinel_service_name="",
            )

    def test_redis_connection_kwargs_from_url_preserves_auth_db_and_ssl(self) -> None:
        kwargs = engine_config._redis_connection_kwargs_from_url(  # noqa: SLF001
            "rediss://user:secret@redis-master:6380/3"
        )

        self.assertEqual(kwargs["username"], "user")
        self.assertEqual(kwargs["password"], "secret")
        self.assertEqual(kwargs["db"], 3)
        self.assertTrue(kwargs["ssl"])
        self.assertFalse(kwargs["decode_responses"])

    def test_sentinel_discovery_remap_maps_docker_gateway_for_local_harness(self) -> None:
        remap = engine_config._sentinel_discovery_remap(  # noqa: SLF001
            (("127.0.0.1", 26379), ("localhost", 26380))
        )

        self.assertIsNotNone(remap)
        assert remap is not None
        self.assertEqual(remap(("host.docker.internal", 6379)), ("127.0.0.1", 6379))
        self.assertEqual(remap(("gateway.docker.internal", 6380)), ("127.0.0.1", 6380))
        self.assertEqual(remap(("redis-master", 6379)), ("redis-master", 6379))

    def test_sentinel_discovery_remap_is_disabled_for_non_loopback_sentinels(self) -> None:
        remap = engine_config._sentinel_discovery_remap(  # noqa: SLF001
            (("sentinel-1", 26379),)
        )

        self.assertIsNone(remap)


class _FakePool:
    def __init__(self) -> None:
        self.aclose_calls = 0
        self.disconnect_calls: list[dict[str, object]] = []

    async def aclose(self) -> None:
        self.aclose_calls += 1

    async def disconnect(self, inuse_connections: bool = False) -> None:
        self.disconnect_calls.append({"inuse_connections": inuse_connections})


class _FakeRedis:
    def __init__(self, pool: object) -> None:
        self.connection_pool = pool
        self.aclose_calls: list[dict[str, object]] = []

    async def aclose(self, close_connection_pool: bool = False) -> None:
        self.aclose_calls.append({"close_connection_pool": close_connection_pool})


class _FakeSentinelManager:
    def __init__(self, sentinels: list[_FakeRedis]) -> None:
        self.sentinels = sentinels


class _FakeSentinelPool(_FakePool):
    def __init__(self, sentinels: list[_FakeRedis]) -> None:
        super().__init__()
        self.sentinel_manager = _FakeSentinelManager(sentinels)


class TestFluxiSentinelClientClosing(unittest.IsolatedAsyncioTestCase):

    async def test_close_redis_client_closes_sentinel_discovery_clients(self) -> None:
        sentinel_one = _FakeRedis(_FakePool())
        sentinel_two = _FakeRedis(_FakePool())
        redis = _FakeRedis(_FakeSentinelPool([sentinel_one, sentinel_two]))

        await engine_config.close_redis_client(redis)

        self.assertEqual(redis.aclose_calls, [{"close_connection_pool": True}])
        self.assertEqual(redis.connection_pool.aclose_calls, 1)
        self.assertEqual(
            redis.connection_pool.disconnect_calls,
            [{"inuse_connections": True}],
        )
        self.assertEqual(
            sentinel_one.aclose_calls,
            [{"close_connection_pool": True}],
        )
        self.assertEqual(
            sentinel_one.connection_pool.disconnect_calls,
            [{"inuse_connections": True}],
        )
        self.assertEqual(
            sentinel_two.aclose_calls,
            [{"close_connection_pool": True}],
        )
        self.assertEqual(
            sentinel_two.connection_pool.disconnect_calls,
            [{"inuse_connections": True}],
        )
