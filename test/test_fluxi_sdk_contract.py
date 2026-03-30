import inspect
from datetime import timedelta
import unittest

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

    def test_connect_requires_explicit_backend_selection(self):
        with self.assertRaises(TypeError):
            WorkflowClient.connect()

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


if __name__ == "__main__":
    unittest.main()
