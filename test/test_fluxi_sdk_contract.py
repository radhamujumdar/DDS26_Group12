import asyncio
import inspect
from datetime import timedelta
import unittest
from unittest import mock

import fluxi_sdk_test_support  # noqa: F401

from fluxi_sdk import (
    ActivityOptions,
    EngineConnectionConfig,
    FakeFluxiRuntime,
    RetryPolicy,
    StartPolicy,
    Worker,
    WorkflowClient,
)
from fluxi_sdk import activity, client, errors, testing, types, worker, workflow
from fluxi_sdk._engine_backend import EngineWorkflowBackend, _EngineWorkerBinding


class TestFluxiSdkContract(unittest.TestCase):

    def test_public_modules_import(self):
        self.assertIsNotNone(activity)
        self.assertIsNotNone(client)
        self.assertIsNotNone(errors)
        self.assertIsNotNone(testing)
        self.assertIsNotNone(types)
        self.assertIsNotNone(worker)
        self.assertIsNotNone(workflow)

    def test_execute_activity_signature(self):
        signature = inspect.signature(workflow.execute_activity)

        self.assertEqual(
            list(signature.parameters),
            [
                "activity",
                "activity_args",
                "task_queue",
                "retry_policy",
                "schedule_to_close_timeout",
                "args",
                "timeout_seconds",
            ],
        )
        self.assertIsNone(signature.parameters["task_queue"].default)
        self.assertIsNone(signature.parameters["retry_policy"].default)
        self.assertIsNone(signature.parameters["schedule_to_close_timeout"].default)
        self.assertIsNone(signature.parameters["args"].default)
        self.assertIsNone(signature.parameters["timeout_seconds"].default)
        self.assertEqual(
            signature.parameters["activity_args"].kind,
            inspect.Parameter.VAR_POSITIONAL,
        )
        self.assertTrue(inspect.iscoroutinefunction(workflow.execute_activity))

    def test_start_activity_signature(self):
        signature = inspect.signature(workflow.start_activity)

        self.assertEqual(
            list(signature.parameters),
            [
                "activity",
                "activity_args",
                "task_queue",
                "retry_policy",
                "schedule_to_close_timeout",
                "args",
                "timeout_seconds",
            ],
        )
        self.assertFalse(inspect.iscoroutinefunction(workflow.start_activity))

    def test_client_execute_workflow_signature(self):
        signature = inspect.signature(WorkflowClient.execute_workflow)

        self.assertEqual(
            list(signature.parameters),
            [
                "self",
                "workflow",
                "workflow_args",
                "id",
                "task_queue",
                "start_policy",
                "workflow_key",
                "args",
            ],
        )
        self.assertIsNone(signature.parameters["task_queue"].default)
        self.assertIsNone(signature.parameters["id"].default)
        self.assertEqual(
            signature.parameters["start_policy"].default,
            StartPolicy.ATTACH_OR_START,
        )
        self.assertIsNone(signature.parameters["workflow_key"].default)
        self.assertIsNone(signature.parameters["args"].default)
        self.assertEqual(
            signature.parameters["workflow_args"].kind,
            inspect.Parameter.VAR_POSITIONAL,
        )
        self.assertTrue(inspect.iscoroutinefunction(WorkflowClient.execute_workflow))

    def test_public_types_are_instantiable(self):
        retry_policy = RetryPolicy(max_attempts=3, initial_interval_seconds=1)
        options = ActivityOptions(
            task_queue="payment",
            retry_policy=retry_policy,
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        self.assertEqual(retry_policy.max_attempts, 3)
        self.assertEqual(options.task_queue, "payment")
        self.assertEqual(options.retry_policy, retry_policy)
        self.assertEqual(options.schedule_to_close_timeout, timedelta(seconds=30))
        self.assertEqual(options.timeout_seconds, 30)

    def test_fake_runtime_is_public(self):
        runtime = FakeFluxiRuntime()
        client_facade = WorkflowClient.connect(runtime=runtime)
        worker_facade = Worker(client_facade, task_queue="orders")

        self.assertIsInstance(runtime, FakeFluxiRuntime)
        self.assertIsInstance(client_facade, WorkflowClient)
        self.assertIsInstance(worker_facade, Worker)
        self.assertIs(runtime.create_client(), runtime.create_client())

    def test_engine_connection_config_is_public(self):
        config = EngineConnectionConfig(
            server_url="http://localhost:8000",
            redis_url="redis://localhost:6379/0",
        )
        self.assertEqual(config.server_url, "http://localhost:8000")
        self.assertEqual(config.redis_url, "redis://localhost:6379/0")
        self.assertEqual(config.result_wait_timeout_ms, 5000)
        self.assertEqual(config.http_connect_timeout_seconds, 2.0)
        self.assertEqual(config.http_read_timeout_seconds, 10.0)
        self.assertEqual(config.http_write_timeout_seconds, 10.0)
        self.assertEqual(config.http_pool_timeout_seconds, 1.0)
        self.assertEqual(config.http_max_connections, 32)
        self.assertEqual(config.http_max_keepalive_connections, 16)
        self.assertEqual(config.http_control_max_connections, 128)
        self.assertEqual(config.http_control_max_keepalive_connections, 64)
        self.assertEqual(config.http_result_max_connections, 256)
        self.assertEqual(config.http_result_max_keepalive_connections, 128)
        self.assertEqual(config.sticky_schedule_to_start_timeout_ms, 5000)
        self.assertEqual(config.sticky_cache_max_runs, 1000)
        self.assertEqual(config.sticky_cache_ttl_ms, 60000)

    def test_connect_requires_explicit_backend_selection(self):
        with self.assertRaises(TypeError):
            WorkflowClient.connect()

    def test_worker_supports_concurrency_configuration(self):
        runtime = FakeFluxiRuntime()
        client_facade = WorkflowClient.connect(runtime=runtime)
        worker_facade = Worker(
            client_facade,
            task_queue="orders",
            max_concurrent_workflow_tasks=2,
            max_concurrent_activity_tasks=3,
        )

        self.assertEqual(worker_facade.max_concurrent_workflow_tasks, 2)
        self.assertEqual(worker_facade.max_concurrent_activity_tasks, 3)

    def test_workflow_decorator_no_longer_declares_task_queue(self):
        @workflow.defn
        class CheckoutWorkflow:

            @workflow.run
            async def run(self, order_id: str) -> str:
                return order_id

        self.assertEqual(CheckoutWorkflow.__name__, "CheckoutWorkflow")
        self.assertTrue(hasattr(CheckoutWorkflow.run, "__fluxi_workflow_class__"))

    def test_workflow_run_requires_async_function(self):
        with self.assertRaises(errors.InvalidWorkflowDefinitionError):

            @workflow.run
            def run_sync():
                return None

    def test_workflow_defn_requires_single_run_method(self):
        with self.assertRaises(errors.InvalidWorkflowDefinitionError):

            @workflow.defn
            class MissingRunWorkflow:
                async def execute(self):
                    return None

    def test_activity_decorator_accepts_optional_name(self):
        @activity.defn(name="reserve_stock")
        def reserve():
            return None

        self.assertEqual(reserve.__name__, "reserve")

    def test_unsafe_imports_passed_through_is_noop_context_manager(self):
        with workflow.unsafe.imports_passed_through():
            value = "ok"

        self.assertEqual(value, "ok")

class TestFluxiSdkEngineHttpClient(unittest.IsolatedAsyncioTestCase):
    async def test_engine_backend_uses_separate_reused_http_clients(self):
        config = EngineConnectionConfig(
            server_url="http://localhost:8000",
            redis_url="redis://localhost:6379/0",
        )
        backend = EngineWorkflowBackend(config)
        created_clients: list[_FakeAsyncClient] = []
        build_calls: list[tuple[int, int]] = []

        def _factory(
            _: EngineConnectionConfig,
            *,
            max_connections: int | None = None,
            max_keepalive_connections: int | None = None,
        ) -> _FakeAsyncClient:
            client = _FakeAsyncClient()
            created_clients.append(client)
            build_calls.append((max_connections or -1, max_keepalive_connections or -1))
            return client

        with mock.patch("fluxi_sdk._engine_backend._build_http_client", side_effect=_factory):
            control_first = await backend._get_control_http_client()
            control_second = await backend._get_control_http_client()
            result_first = await backend._get_result_http_client()
            result_second = await backend._get_result_http_client()
            self.assertIs(control_first, control_second)
            self.assertIs(result_first, result_second)
            self.assertIsNot(control_first, result_first)
            self.assertEqual(len(created_clients), 2)
            self.assertEqual(
                build_calls,
                [
                    (
                        config.http_control_max_connections,
                        config.http_control_max_keepalive_connections,
                    ),
                    (
                        config.http_result_max_connections,
                        config.http_result_max_keepalive_connections,
                    ),
                ],
            )
            await backend.aclose()
            self.assertTrue(control_first.is_closed)
            self.assertTrue(result_first.is_closed)


class TestFluxiSdkEngineHistoryCache(unittest.IsolatedAsyncioTestCase):
    async def test_worker_history_fetch_uses_after_index_on_cache_hit(self):
        config = EngineConnectionConfig(
            server_url="http://localhost:8000",
            redis_url="redis://localhost:6379/0",
        )
        binding = _EngineWorkerBinding(
            config=config,
            task_queue="orders",
            workflows=(),
            activities=(),
            max_concurrent_workflow_tasks=1,
            max_concurrent_activity_tasks=1,
        )
        fake_client = _HistoryClient(
            [
                _HistoryResponse(
                    {
                        "events": [
                            {"event_type": "WorkflowStarted", "input_payload": None},
                            {"event_type": "ActivityScheduled", "input_payload": None},
                        ],
                        "next_index": 1,
                    }
                ),
                _HistoryResponse(
                    {
                        "events": [
                            {
                                "event_type": "ActivityCompleted",
                                "result_payload": None,
                                "activity_execution_id": "run-1:act:1",
                            }
                        ],
                        "next_index": 2,
                    }
                ),
            ]
        )
        binding._http_client = fake_client

        first = await binding._fetch_run_history("run-1")
        second = await binding._fetch_run_history("run-1")

        self.assertEqual([event["event_type"] for event in first], ["WorkflowStarted", "ActivityScheduled"])
        self.assertEqual(
            [event["event_type"] for event in second],
            ["WorkflowStarted", "ActivityScheduled", "ActivityCompleted"],
        )
        self.assertEqual(
            fake_client.calls,
            [
                ("/runs/run-1/history", None),
                ("/runs/run-1/history", {"after_index": 1}),
            ],
        )

    async def test_worker_history_fetch_serializes_same_run_tail_requests(self):
        config = EngineConnectionConfig(
            server_url="http://localhost:8000",
            redis_url="redis://localhost:6379/0",
        )
        binding = _EngineWorkerBinding(
            config=config,
            task_queue="orders",
            workflows=(),
            activities=(),
            max_concurrent_workflow_tasks=1,
            max_concurrent_activity_tasks=1,
        )
        fake_client = _HistoryClient(
            [
                _HistoryResponse(
                    {
                        "events": [
                            {"event_type": "WorkflowStarted", "input_payload": None},
                            {"event_type": "ActivityScheduled", "input_payload": None},
                        ],
                        "next_index": 1,
                    }
                ),
                _HistoryResponse(
                    {
                        "events": [
                            {
                                "event_type": "ActivityCompleted",
                                "result_payload": None,
                                "activity_execution_id": "run-1:act:1",
                            }
                        ],
                        "next_index": 2,
                    }
                ),
                _HistoryResponse({"events": [], "next_index": 2}),
            ]
        )
        binding._http_client = fake_client

        await binding._fetch_run_history("run-1")
        second, third = await asyncio.gather(
            binding._fetch_run_history("run-1"),
            binding._fetch_run_history("run-1"),
        )

        self.assertEqual(
            [event["event_type"] for event in second],
            ["WorkflowStarted", "ActivityScheduled", "ActivityCompleted"],
        )
        self.assertEqual(
            [event["event_type"] for event in third],
            ["WorkflowStarted", "ActivityScheduled", "ActivityCompleted"],
        )
        self.assertEqual(
            fake_client.calls,
            [
                ("/runs/run-1/history", None),
                ("/runs/run-1/history", {"after_index": 1}),
                ("/runs/run-1/history", {"after_index": 2}),
            ],
        )


class _FakeAsyncClient:
    def __init__(self) -> None:
        self.is_closed = False

    async def aclose(self) -> None:
        self.is_closed = True


class _HistoryResponse:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._body


class _HistoryClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def get(self, path, params=None):
        self.calls.append((path, params))
        return self._responses.pop(0)


if __name__ == "__main__":
    unittest.main()
