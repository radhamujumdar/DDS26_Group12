import os
from unittest import TestCase
from unittest.mock import patch

import fluxi_sdk_test_support  # noqa: F401

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
