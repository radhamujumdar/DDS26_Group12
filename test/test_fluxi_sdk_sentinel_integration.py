from __future__ import annotations

import asyncio
from datetime import timedelta
import threading
import time
import unittest

import fluxi_sdk_test_support  # noqa: F401
import httpx
import uvicorn

from fluxi_engine import FluxiRedisStore, FluxiSettings, create_app, create_redis_client
from fluxi_engine.scheduler import FluxiScheduler
from fluxi_engine_test_support import SentinelHarness, _find_free_port
from fluxi_sdk import EngineConnectionConfig, Worker, activity, workflow
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.examples.checkout import (
    CheckoutItem,
    CheckoutOrder,
    PaymentReceipt,
    ReferenceCheckoutState,
    ReferenceCheckoutWorkflow,
    create_reference_checkout_engine_environment,
)
from fluxi_sdk.types import RetryPolicy


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


class FluxiSdkSentinelAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    harness: SentinelHarness

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.harness = SentinelHarness()
        cls.harness.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.harness.stop()
        super().tearDownClass()

    async def asyncSetUp(self) -> None:
        self.settings = FluxiSettings(
            redis_mode="sentinel",
            redis_url=self.harness.redis_url,
            sentinel_endpoints=self.harness.sentinel_endpoints,
            sentinel_service_name=self.harness.service_name,
            key_prefix=f"fluxi-sdk-sentinel-test:{self._testMethodName}",
            workflow_task_timeout_ms=50,
            result_poll_interval_ms=10,
            timer_poll_interval_ms=10,
            pending_idle_threshold_ms=1,
            pending_claim_count=100,
        )
        self.assertion_store = FluxiRedisStore(
            create_redis_client(self.settings),
            self.settings,
        )
        await self._wait_until_redis_ready()
        await self.assertion_store.flushdb()

        self.server = _UvicornServerThread(create_app(self.settings))
        self.server.start()
        await self._wait_until_server_ready()

        self.scheduler = FluxiScheduler.from_settings(self.settings)
        self.scheduler_task = asyncio.create_task(self.scheduler.run_forever())

        self.engine = EngineConnectionConfig(
            server_url=self.server.base_url,
            redis_mode="sentinel",
            redis_url=self.settings.redis_url,
            sentinel_endpoints=self.settings.sentinel_endpoints,
            sentinel_service_name=self.settings.sentinel_service_name,
            sentinel_min_other_sentinels=self.settings.sentinel_min_other_sentinels,
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
        await self.assertion_store.flushdb()
        await self.assertion_store.aclose()

    async def _wait_until_redis_ready(self) -> None:
        deadline = time.time() + 30
        while True:
            try:
                await self.assertion_store.redis.ping()
                return
            except Exception:
                if time.time() >= deadline:
                    raise
                await asyncio.sleep(0.1)

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


class TestFluxiSdkSentinelIntegration(FluxiSdkSentinelAsyncTestCase):

    async def test_reference_checkout_workers_run_through_sentinel(self) -> None:
        state = ReferenceCheckoutState(
            orders={
                "order-1": CheckoutOrder(
                    order_id="order-1",
                    user_id="user-1",
                    total_cost=20,
                    items=(CheckoutItem(item_id="item-1", quantity=2),),
                ),
            },
            stock_levels={"item-1": 5},
            user_credit={"user-1": 25},
        )
        client, workers = create_reference_checkout_engine_environment(state, self.engine)

        async with workers[0], workers[1], workers[2]:
            old_master = self.harness.wait_until_ready()
            new_master = self.harness.force_failover()
            self.assertNotEqual(old_master, new_master)

            result = await client.execute_workflow(
                ReferenceCheckoutWorkflow.run,
                "order-1",
                id="checkout:order-1",
                task_queue="orders",
            )

        self.assertEqual(result.order_id, "order-1")
        self.assertEqual(result.status, "paid")
        self.assertEqual(
            result.payment,
            PaymentReceipt(payment_id="payment-1", user_id="user-1", amount=20),
        )

    async def test_active_workflow_completes_after_failover(self) -> None:
        entered = asyncio.Event()
        release = asyncio.Event()

        @activity.defn(name="hold_step")
        async def hold_step() -> str:
            entered.set()
            await release.wait()
            return "ok"

        @workflow.defn(name="HoldWorkflow")
        class HoldWorkflow:

            @workflow.run
            async def run(self) -> str:
                return await workflow.execute_activity("hold_step", task_queue="orders")

        client = WorkflowClient.connect(engine=self.engine)
        worker = Worker(
            client,
            task_queue="orders",
            workflows=[HoldWorkflow],
            activities=[hold_step],
        )

        async with worker:
            execution = asyncio.create_task(
                client.execute_workflow(
                    HoldWorkflow.run,
                    id="hold:1",
                    task_queue="orders",
                )
            )
            await asyncio.wait_for(entered.wait(), timeout=10)
            old_master = self.harness.wait_until_ready()
            new_master = self.harness.force_failover()
            self.assertNotEqual(old_master, new_master)
            release.set()
            result = await asyncio.wait_for(execution, timeout=20)

        self.assertEqual(result, "ok")

    async def test_activity_timeout_retry_survives_failover(self) -> None:
        attempts = {"count": 0}
        first_attempt_started = asyncio.Event()

        @activity.defn(name="flaky_charge")
        async def flaky_charge() -> str:
            attempts["count"] += 1
            if attempts["count"] == 1:
                first_attempt_started.set()
                await asyncio.sleep(0.12)
            return "charged"

        @workflow.defn(name="RetryWorkflowSentinel")
        class RetryWorkflowSentinel:

            @workflow.run
            async def run(self) -> str:
                return await workflow.execute_activity(
                    "flaky_charge",
                    task_queue="payment",
                    retry_policy=RetryPolicy(
                        max_attempts=2,
                        initial_interval_seconds=0.01,
                    ),
                    schedule_to_close_timeout=timedelta(seconds=0.02),
                )

        client = WorkflowClient.connect(engine=self.engine)
        workflow_worker = Worker(
            client,
            task_queue="orders",
            workflows=[RetryWorkflowSentinel],
        )
        activity_worker = Worker(
            client,
            task_queue="payment",
            activities=[flaky_charge],
        )

        async with workflow_worker, activity_worker:
            execution = asyncio.create_task(
                client.execute_workflow(
                    RetryWorkflowSentinel.run,
                    id="retry:sentinel:1",
                    task_queue="orders",
                )
            )
            await asyncio.wait_for(first_attempt_started.wait(), timeout=10)
            old_master = self.harness.wait_until_ready()
            new_master = self.harness.force_failover()
            self.assertNotEqual(old_master, new_master)
            result = await asyncio.wait_for(execution, timeout=20)

        self.assertEqual(result, "charged")
        self.assertEqual(attempts["count"], 2)
