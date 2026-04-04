from __future__ import annotations

from dataclasses import dataclass
import unittest

import httpx
from redis.exceptions import ResponseError

from fluxi_engine import FluxiSettings, create_app


@dataclass
class _ReadyFailureStore:
    async def ping(self, *, timeout_seconds: float = 30.0) -> bool:
        raise ResponseError("LOADING Redis is loading the dataset in memory")

    async def aclose(self) -> None:
        return None


@dataclass
class _StartFailureStore:
    async def ping(self, *, timeout_seconds: float = 30.0) -> bool:
        return True

    async def aclose(self) -> None:
        return None

    async def start_or_attach_workflow(self, **_: object):
        raise ResponseError("LOADING Redis is loading the dataset in memory")


class TestFluxiEngineServerResilience(unittest.IsolatedAsyncioTestCase):
    async def test_readyz_returns_503_while_store_is_loading(self) -> None:
        app = create_app(FluxiSettings(), store=_ReadyFailureStore())  # type: ignore[arg-type]
        app.state.store = _ReadyFailureStore()
        app.state.settings = FluxiSettings()
        transport = httpx.ASGITransport(app=app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/readyz")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "Fluxi store temporarily unavailable.")

    async def test_start_or_attach_returns_503_while_store_is_loading(self) -> None:
        store = _StartFailureStore()
        app = create_app(FluxiSettings(), store=store)  # type: ignore[arg-type]
        app.state.store = store
        app.state.settings = FluxiSettings()
        transport = httpx.ASGITransport(app=app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.post(
                "/workflows/start-or-attach",
                json={
                    "workflow_id": "checkout:1",
                    "workflow_name": "CheckoutWorkflow",
                    "task_queue": "orders",
                    "start_policy": "attach_or_start",
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "Fluxi store temporarily unavailable.")
