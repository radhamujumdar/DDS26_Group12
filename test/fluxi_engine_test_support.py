from __future__ import annotations

import asyncio
import base64
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import unittest

import httpx

REPO_ROOT = Path(__file__).resolve().parents[1]
ENGINE_SRC = REPO_ROOT / "packages" / "fluxi-engine" / "src"
if str(ENGINE_SRC) not in sys.path:
    sys.path.insert(0, str(ENGINE_SRC))

from fluxi_engine import FluxiRedisStore, FluxiSettings, create_app, create_redis_client
from fluxi_engine.codecs import packb, unpackb
from fluxi_engine.keys import activity_queue, workflow_queue


def _find_free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class RedisHarness:
    def __init__(self) -> None:
        self._container_id: str | None = None
        self._owns_container = False
        self.url: str | None = None
        self.port: int | None = None

    def start(self) -> None:
        explicit_url = os.getenv("FLUXI_TEST_REDIS_URL")
        if explicit_url:
            self.url = explicit_url
            return

        docker = shutil.which("docker")
        if docker is None:
            raise unittest.SkipTest("Docker is not available for Fluxi engine integration tests.")

        try:
            subprocess.run(
                [docker, "info"],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except Exception as exc:
            raise unittest.SkipTest(
                f"Docker is unavailable for Fluxi engine integration tests: {exc}"
            ) from exc

        self.port = _find_free_port()
        result = subprocess.run(
            [
                docker,
                "run",
                "--rm",
                "-d",
                "-p",
                f"{self.port}:6379",
                "redis:7.2-bookworm",
                "redis-server",
                "--save",
                "",
                "--appendonly",
                "no",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self._container_id = result.stdout.strip()
        self._owns_container = True
        self.url = f"redis://127.0.0.1:{self.port}/0"

    def stop(self) -> None:
        if not self._owns_container or not self._container_id:
            return
        docker = shutil.which("docker")
        if docker is None:
            return
        subprocess.run(
            [docker, "rm", "-f", self._container_id],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self._container_id = None


class FluxiEngineAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    harness: RedisHarness
    settings: FluxiSettings
    store: FluxiRedisStore
    client: httpx.AsyncClient

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.harness = RedisHarness()
        cls.harness.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.harness.stop()
        super().tearDownClass()

    async def asyncSetUp(self) -> None:
        if self.harness.url is None:
            raise unittest.SkipTest("No Redis URL available for engine tests.")

        self.settings = FluxiSettings(
            redis_url=self.harness.url,
            key_prefix=f"fluxi-test:{self._testMethodName}",
            workflow_task_timeout_ms=50,
            result_poll_interval_ms=10,
            timer_poll_interval_ms=10,
            pending_idle_threshold_ms=1,
            pending_claim_count=100,
        )
        redis = create_redis_client(self.settings)
        self.store = FluxiRedisStore(redis, self.settings)
        await self._wait_until_redis_ready()
        await self.store.flushdb()

        app = create_app(self.settings, store=self.store)
        app.state.store = self.store
        app.state.settings = self.settings
        transport = httpx.ASGITransport(app=app)
        self.client = httpx.AsyncClient(transport=transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        await self.store.flushdb()
        await self.store.aclose()

    async def _wait_until_redis_ready(self) -> None:
        deadline = time.time() + 15
        while True:
            try:
                await self.store.redis.ping()
                return
            except Exception:
                if time.time() >= deadline:
                    raise
                await asyncio.sleep(0.1)

    async def start_workflow(
        self,
        *,
        workflow_id: str = "checkout:1",
        workflow_name: str = "CheckoutWorkflow",
        task_queue: str = "orders",
        start_policy: str = "allow_duplicate",
        input_payload: dict | None = None,
    ) -> dict:
        response = await self.client.post(
            "/workflows/start-or-attach",
            json={
                "workflow_id": workflow_id,
                "workflow_name": workflow_name,
                "task_queue": task_queue,
                "start_policy": start_policy,
                "input_payload_b64": self.payload_b64(
                    input_payload or {"order_id": workflow_id}
                ),
            },
        )
        response.raise_for_status()
        return response.json()

    async def latest_stream_payload(self, stream_key: str) -> tuple[str, dict]:
        entries = await self.store.redis.xrange(stream_key)
        entry_id, values = entries[-1]
        payload = values[b"payload"] if b"payload" in values else values["payload"]
        return (entry_id.decode("utf-8") if isinstance(entry_id, bytes) else entry_id, unpackb(payload))

    async def read_group_message(
        self,
        *,
        stream_key: str,
        group_name: str,
        consumer_name: str,
    ) -> tuple[str, dict]:
        messages = await self.store.redis.xreadgroup(
            group_name,
            consumer_name,
            {stream_key: ">"},
            count=1,
            block=100,
        )
        stream_messages = messages[0][1]
        message_id, values = stream_messages[0]
        payload = values[b"payload"] if b"payload" in values else values["payload"]
        return (message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id, unpackb(payload))

    def workflow_stream_key(self, task_queue: str = "orders") -> str:
        return workflow_queue(self.settings.key_prefix, task_queue)

    def activity_stream_key(self, task_queue: str) -> str:
        return activity_queue(self.settings.key_prefix, task_queue)

    def payload_b64(self, payload: object) -> str:
        return base64.b64encode(packb(payload)).decode("ascii")
