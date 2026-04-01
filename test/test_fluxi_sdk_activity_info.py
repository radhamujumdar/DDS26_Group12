from __future__ import annotations

import asyncio
import threading
import time
import unittest

import fluxi_sdk_test_support  # noqa: F401
import httpx
import uvicorn

from fluxi_engine import FluxiRedisStore, FluxiSettings, create_app, create_redis_client
from fluxi_engine.scheduler import FluxiScheduler
from fluxi_engine_test_support import RedisHarness, _find_free_port
from fluxi_sdk import EngineConnectionConfig, Worker, activity, workflow
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.errors import ActivityContextUnavailableError
from fluxi_sdk.testing import FakeFluxiRuntime


class _UvicornServerThread:
    def __init__(self, app) -> None:
        self.port = _find_free_port()
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
            access_log=False,
        )
        self.server = uvicorn.Server(config)
        self.server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=10)


class TestActivityInfoFakeRuntime(unittest.IsolatedAsyncioTestCase):
    def test_activity_info_raises_outside_activity_context(self) -> None:
        with self.assertRaises(ActivityContextUnavailableError):
            activity.info()

    async def test_activity_info_is_available_in_fake_runtime(self) -> None:
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)
        captured: dict[str, object] = {}

        @activity.defn(name="capture_activity_info")
        def capture_activity_info() -> str:
            info = activity.info()
            captured.update(
                {
                    "activity_execution_id": info.activity_execution_id,
                    "attempt_no": info.attempt_no,
                    "activity_name": info.activity_name,
                    "task_queue": info.task_queue,
                    "workflow_id": info.workflow_id,
                    "run_id": info.run_id,
                }
            )
            return info.activity_execution_id

        @workflow.defn(name="ActivityInfoWorkflow")
        class ActivityInfoWorkflow:
            @workflow.run
            async def run(self) -> str:
                return await workflow.execute_activity(
                    "capture_activity_info",
                    task_queue="orders",
                )

        worker = Worker(
            client,
            task_queue="orders",
            workflows=[ActivityInfoWorkflow],
            activities=[capture_activity_info],
        )

        async with worker:
            result = await client.execute_workflow(
                ActivityInfoWorkflow.run,
                id="activity-info:fake",
                task_queue="orders",
            )

        self.assertEqual(result, captured["activity_execution_id"])
        self.assertEqual(captured["attempt_no"], 1)
        self.assertEqual(captured["activity_name"], "capture_activity_info")
        self.assertEqual(captured["task_queue"], "orders")
        self.assertEqual(captured["workflow_id"], "activity-info:fake")
        self.assertTrue(str(captured["run_id"]).startswith("run-"))


class TestActivityInfoEngineRuntime(unittest.IsolatedAsyncioTestCase):
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
            raise unittest.SkipTest("No Redis URL available for engine activity-info test.")
        self.settings = FluxiSettings(
            redis_url=self.harness.url,
            key_prefix=f"fluxi-activity-info:{self._testMethodName}",
            workflow_task_timeout_ms=5000,
            result_poll_interval_ms=10,
            timer_poll_interval_ms=10,
            pending_idle_threshold_ms=1,
            pending_claim_count=100,
        )
        self.store = FluxiRedisStore(create_redis_client(self.settings), self.settings)
        await self.store.flushdb()
        self.server = _UvicornServerThread(create_app(self.settings))
        self.server.start()
        await self._wait_until_server_ready()
        self.scheduler = FluxiScheduler.from_settings(self.settings)
        self.scheduler_task = asyncio.create_task(self.scheduler.run_forever())
        self.engine = EngineConnectionConfig(
            server_url=self.server.base_url,
            redis_url=self.settings.redis_url,
            key_prefix=self.settings.key_prefix,
            workflow_consumer_group=self.settings.workflow_consumer_group,
            activity_consumer_group=self.settings.activity_consumer_group,
            result_poll_interval_ms=self.settings.result_poll_interval_ms,
        )

    async def asyncTearDown(self) -> None:
        await self.scheduler.stop()
        self.scheduler_task.cancel()
        await asyncio.gather(self.scheduler_task, return_exceptions=True)
        await self.scheduler.aclose()
        self.server.stop()
        await self.store.flushdb()
        await self.store.aclose()

    async def _wait_until_server_ready(self) -> None:
        deadline = time.time() + 15
        async with httpx.AsyncClient(base_url=self.server.base_url) as client:
            while True:
                try:
                    response = await client.get("/readyz")
                    if response.status_code == 200:
                        return
                except Exception:
                    pass
                if time.time() >= deadline:
                    raise TimeoutError("Timed out waiting for fluxi-server to become ready.")
                await asyncio.sleep(0.1)

    async def test_activity_info_is_available_in_engine_runtime(self) -> None:
        client = WorkflowClient.connect(engine=self.engine)
        captured: dict[str, object] = {}

        @activity.defn(name="capture_remote_activity_info")
        async def capture_remote_activity_info() -> str:
            info = activity.info()
            captured.update(
                {
                    "activity_execution_id": info.activity_execution_id,
                    "attempt_no": info.attempt_no,
                    "activity_name": info.activity_name,
                    "task_queue": info.task_queue,
                    "workflow_id": info.workflow_id,
                    "run_id": info.run_id,
                }
            )
            return info.activity_execution_id

        @workflow.defn(name="RemoteActivityInfoWorkflow")
        class RemoteActivityInfoWorkflow:
            @workflow.run
            async def run(self) -> str:
                return await workflow.execute_activity(
                    "capture_remote_activity_info",
                    task_queue="orders",
                )

        worker = Worker(
            client,
            task_queue="orders",
            workflows=[RemoteActivityInfoWorkflow],
            activities=[capture_remote_activity_info],
        )

        async with worker:
            result = await client.execute_workflow(
                RemoteActivityInfoWorkflow.run,
                id="activity-info:engine",
                task_queue="orders",
            )

        self.assertEqual(result, captured["activity_execution_id"])
        self.assertEqual(captured["attempt_no"], 1)
        self.assertEqual(captured["activity_name"], "capture_remote_activity_info")
        self.assertEqual(captured["task_queue"], "orders")
        self.assertEqual(captured["workflow_id"], "activity-info:engine")
        self.assertTrue(str(captured["run_id"]).startswith("run-"))
