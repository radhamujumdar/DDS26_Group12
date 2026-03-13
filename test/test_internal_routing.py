import importlib.util
import logging
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

ROOT = Path(__file__).resolve().parents[1]


class ModuleSandbox:
    def __init__(self) -> None:
        self._loaded: list[str] = []

    def add_package(self, name: str) -> None:
        package = types.ModuleType(name)
        package.__path__ = []  # type: ignore[attr-defined]
        sys.modules[name] = package
        self._loaded.append(name)

    def load(self, name: str, path: Path):
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        self._loaded.append(name)
        spec.loader.exec_module(module)
        if "." in name:
            parent_name, child_name = name.rsplit(".", 1)
            parent = sys.modules[parent_name]
            setattr(parent, child_name, module)
        return module

    def cleanup(self) -> None:
        for name in reversed(self._loaded):
            sys.modules.pop(name, None)


class RecordingAsyncClient:
    def __init__(self, payload: dict[str, str] | None = None, status_code: int = 200) -> None:
        self.payload = payload or {"state": "COMMITTED"}
        self.status_code = status_code
        self.calls: list[str] = []

    async def get(self, url: str) -> httpx.Response:
        self.calls.append(url)
        request = httpx.Request("GET", url)
        return httpx.Response(self.status_code, json=self.payload, request=request)


class InternalRoutingTests(unittest.IsolatedAsyncioTestCase):
    def test_order_config_defaults_to_direct_service_urls(self) -> None:
        sandbox = ModuleSandbox()
        try:
            sandbox.load("models", ROOT / "order/models.py")
            config_module = sandbox.load("order_config_test", ROOT / "order/config.py")
            with patch.dict(
                os.environ,
                {
                    "REDIS_HOST": "order-db",
                    "REDIS_PORT": "6379",
                    "REDIS_PASSWORD": "redis",
                    "REDIS_DB": "0",
                },
                clear=False,
            ):
                config = config_module.OrderConfig.from_env()
        finally:
            sandbox.cleanup()

        self.assertEqual(config.stock_service_url, "http://stock-service:5000")
        self.assertEqual(config.payment_service_url, "http://payment-service:5000")

    def test_order_clients_use_direct_service_urls(self) -> None:
        sandbox = ModuleSandbox()
        try:
            sandbox.add_package("clients")
            sandbox.load("logging_utils", ROOT / "order/logging_utils.py")
            sandbox.load("models", ROOT / "order/models.py")
            sandbox.load("clients.saga_bus", ROOT / "order/clients/saga_bus.py")
            payment_module = sandbox.load("clients.payment_client", ROOT / "order/clients/payment_client.py")
            stock_module = sandbox.load("clients.stock_client", ROOT / "order/clients/stock_client.py")

            session = object()
            payment_client = payment_module.PaymentClient(session, "http://payment-service:5000/")
            stock_client = stock_module.StockClient(session, "http://stock-service:5000/")
        finally:
            sandbox.cleanup()

        self.assertEqual(payment_client.base_url, "http://payment-service:5000")
        self.assertEqual(stock_client.base_url, "http://stock-service:5000")

    def test_payment_config_defaults_to_order_service_url(self) -> None:
        sandbox = ModuleSandbox()
        try:
            config_module = sandbox.load("payment_config_test", ROOT / "payment/config.py")
            with patch.dict(
                os.environ,
                {
                    "REDIS_HOST": "payment-db",
                    "REDIS_PORT": "6379",
                    "REDIS_PASSWORD": "redis",
                    "REDIS_DB": "0",
                },
                clear=False,
            ):
                config = config_module.PaymentConfig.from_env()
        finally:
            sandbox.cleanup()

        self.assertEqual(config.order_service_url, "http://order-service:5000")

    def test_stock_config_defaults_to_order_service_url(self) -> None:
        sandbox = ModuleSandbox()
        try:
            config_module = sandbox.load("stock_config_test", ROOT / "stock/config.py")
            with patch.dict(
                os.environ,
                {
                    "REDIS_HOST": "stock-db",
                    "REDIS_PORT": "6379",
                    "REDIS_PASSWORD": "redis",
                    "REDIS_DB": "0",
                },
                clear=False,
            ):
                config = config_module.StockConfig.from_env()
        finally:
            sandbox.cleanup()

        self.assertEqual(config.order_service_url, "http://order-service:5000")

    async def test_payment_recovery_queries_order_service_directly(self) -> None:
        sandbox = ModuleSandbox()
        try:
            sandbox.add_package("repository")
            sandbox.add_package("services")
            sandbox.load("logging_utils", ROOT / "payment/logging_utils.py")
            sandbox.load("models", ROOT / "payment/models.py")
            sandbox.load("repository.payment_repo", ROOT / "payment/repository/payment_repo.py")
            sandbox.load("services.payment_service", ROOT / "payment/services/payment_service.py")
            recovery_module = sandbox.load(
                "payment_recovery_service_test",
                ROOT / "payment/services/recovery_service.py",
            )

            client = RecordingAsyncClient({"state": "COMMITTED"})
            service = recovery_module.PaymentRecoveryService(
                repo=types.SimpleNamespace(),
                payment_service=types.SimpleNamespace(),
                order_client=client,
                order_service_url="http://order-service:5000/",
                logger=logging.getLogger("payment-test"),
            )
            state = await service._get_coordinator_tx_state("tx-123")
        finally:
            sandbox.cleanup()

        self.assertEqual(state, "COMMITTED")
        self.assertEqual(client.calls, ["http://order-service:5000/2pc/tx/tx-123"])

    async def test_stock_recovery_queries_order_service_directly(self) -> None:
        sandbox = ModuleSandbox()
        try:
            sandbox.add_package("repository")
            sandbox.add_package("services")
            sandbox.load("logging_utils", ROOT / "stock/logging_utils.py")
            sandbox.load("models", ROOT / "stock/models.py")
            sandbox.load("repository.stock_repo", ROOT / "stock/repository/stock_repo.py")
            sandbox.load("services.stock_service", ROOT / "stock/services/stock_service.py")
            recovery_module = sandbox.load(
                "stock_recovery_service_test",
                ROOT / "stock/services/recovery_service.py",
            )

            client = RecordingAsyncClient({"state": "ABORTED"})
            service = recovery_module.StockRecoveryService(
                repo=types.SimpleNamespace(),
                stock_service=types.SimpleNamespace(),
                order_client=client,
                order_service_url="http://order-service:5000/",
                logger=logging.getLogger("stock-test"),
            )
            state = await service._get_coordinator_tx_state("tx-456")
        finally:
            sandbox.cleanup()

        self.assertEqual(state, "ABORTED")
        self.assertEqual(client.calls, ["http://order-service:5000/2pc/tx/tx-456"])


if __name__ == "__main__":
    unittest.main()
