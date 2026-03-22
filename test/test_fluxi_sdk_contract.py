import inspect
import unittest

from fluxi_sdk import (
    ActivityOptions,
    FakeFluxiRuntime,
    RetryPolicy,
    StartPolicy,
    WorkflowClient,
)
from fluxi_sdk import activity, client, errors, testing, types, workflow


class TestFluxiSdkContract(unittest.TestCase):

    def test_public_modules_import(self):
        self.assertIsNotNone(activity)
        self.assertIsNotNone(client)
        self.assertIsNotNone(errors)
        self.assertIsNotNone(testing)
        self.assertIsNotNone(types)
        self.assertIsNotNone(workflow)

    def test_execute_activity_signature(self):
        signature = inspect.signature(workflow.execute_activity)

        self.assertEqual(
            list(signature.parameters),
            ["name", "args", "task_queue", "retry_policy", "timeout_seconds"],
        )
        self.assertEqual(signature.parameters["args"].default, ())
        self.assertIsNone(signature.parameters["task_queue"].default)
        self.assertIsNone(signature.parameters["retry_policy"].default)
        self.assertIsNone(signature.parameters["timeout_seconds"].default)
        self.assertTrue(inspect.iscoroutinefunction(workflow.execute_activity))

    def test_client_execute_workflow_signature(self):
        signature = inspect.signature(WorkflowClient.execute_workflow)

        self.assertEqual(
            list(signature.parameters),
            ["self", "workflow", "workflow_key", "args", "start_policy"],
        )
        self.assertEqual(signature.parameters["args"].default, ())
        self.assertEqual(
            signature.parameters["start_policy"].default,
            StartPolicy.ATTACH_OR_START,
        )
        self.assertTrue(inspect.iscoroutinefunction(WorkflowClient.execute_workflow))

    def test_public_types_are_instantiable(self):
        retry_policy = RetryPolicy(max_attempts=3, initial_interval_seconds=1)
        options = ActivityOptions(
            task_queue="payment",
            retry_policy=retry_policy,
            timeout_seconds=30,
        )

        self.assertEqual(retry_policy.max_attempts, 3)
        self.assertEqual(options.task_queue, "payment")
        self.assertEqual(options.retry_policy, retry_policy)
        self.assertEqual(options.timeout_seconds, 30)

    def test_fake_runtime_is_public(self):
        runtime = FakeFluxiRuntime()

        self.assertIsInstance(runtime, FakeFluxiRuntime)
        self.assertIs(runtime.create_client(), runtime.create_client())

    def test_workflow_decorator_supports_default_task_queue(self):
        @workflow.defn(default_task_queue="orders")
        class CheckoutWorkflow:

            @workflow.run
            async def run(self, order_id: str) -> str:
                return order_id

        self.assertEqual(CheckoutWorkflow.__name__, "CheckoutWorkflow")

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


if __name__ == "__main__":
    unittest.main()
