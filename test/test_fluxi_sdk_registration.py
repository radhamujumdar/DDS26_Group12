import unittest

from fluxi_sdk import activity, errors, workflow
from fluxi_sdk.types import RetryPolicy


class TestFluxiSdkRegistration(unittest.TestCase):

    @staticmethod
    def _workflow_definition():
        @workflow.defn(default_task_queue="orders")
        class CheckoutWorkflow:

            @workflow.run
            async def run(self, order_id: str) -> str:
                return order_id

        return CheckoutWorkflow

    @staticmethod
    def _activity_definition():
        @activity.defn(name="reserve_stock")
        def reserve_stock(order_id: str) -> str:
            return order_id

        return reserve_stock

    def test_workflow_registry_registers_by_class_and_name(self):
        registry = workflow.WorkflowRegistry()
        workflow_cls = self._workflow_definition()

        registration = registry.register(workflow_cls)

        self.assertIs(registration.workflow, workflow_cls)
        self.assertEqual(registration.name, "CheckoutWorkflow")
        self.assertEqual(registration.run_method_name, "run")
        self.assertEqual(registration.default_task_queue, "orders")
        self.assertIs(registry.get(workflow_cls), registration)
        self.assertIs(registry.get("CheckoutWorkflow"), registration)

    def test_workflow_registry_rejects_undeclared_workflows(self):
        registry = workflow.WorkflowRegistry()

        class UndecoratedWorkflow:
            async def run(self) -> None:
                return None

        with self.assertRaises(errors.InvalidWorkflowDefinitionError):
            registry.register(UndecoratedWorkflow)

        with self.assertRaises(errors.UnknownWorkflowError):
            registry.get("missing-workflow")

    def test_workflow_registry_rejects_duplicates(self):
        registry = workflow.WorkflowRegistry()
        workflow_cls = self._workflow_definition()

        registry.register(workflow_cls)

        with self.assertRaises(errors.DuplicateWorkflowRegistrationError):
            registry.register(workflow_cls)

    def test_activity_registry_registers_by_name_and_callable(self):
        registry = activity.ActivityRegistry()
        activity_fn = self._activity_definition()

        registration = registry.register(activity_fn)

        self.assertIs(registration.fn, activity_fn)
        self.assertEqual(registration.name, "reserve_stock")
        self.assertIs(registry.get(activity_fn), registration)
        self.assertIs(registry.get("reserve_stock"), registration)

    def test_activity_registry_rejects_undeclared_activities(self):
        registry = activity.ActivityRegistry()

        def undecorated_activity() -> None:
            return None

        with self.assertRaises(errors.InvalidActivityDefinitionError):
            registry.register(undecorated_activity)

        with self.assertRaises(errors.UnknownActivityError):
            registry.get("missing-activity")

    def test_activity_registry_rejects_duplicates(self):
        registry = activity.ActivityRegistry()
        activity_fn = self._activity_definition()

        registry.register(activity_fn)

        with self.assertRaises(errors.DuplicateActivityRegistrationError):
            registry.register(activity_fn)


class TestWorkflowExecutionContext(unittest.IsolatedAsyncioTestCase):

    @staticmethod
    def _workflow_registration():
        registry = workflow.WorkflowRegistry()

        @workflow.defn(default_task_queue="orders")
        class CheckoutWorkflow:

            @workflow.run
            async def run(self, order_id: str) -> str:
                return order_id

        return registry.register(CheckoutWorkflow)

    async def test_execute_activity_requires_active_workflow_context(self):
        with self.assertRaises(errors.WorkflowContextUnavailableError):
            await workflow.execute_activity("reserve_stock")

    async def test_execute_activity_uses_workflow_default_task_queue(self):
        registration = self._workflow_registration()
        captured: dict[str, object] = {}

        async def execute_activity(name, args, options):
            captured["name"] = name
            captured["args"] = args
            captured["options"] = options
            return {"scheduled": name, "task_queue": options.task_queue}

        with workflow._activate_execution_context(registration, execute_activity):
            result = await workflow.execute_activity(
                "reserve_stock",
                args=("order-1",),
                retry_policy=RetryPolicy(max_attempts=3),
                timeout_seconds=30,
            )

        self.assertEqual(
            result,
            {"scheduled": "reserve_stock", "task_queue": "orders"},
        )
        self.assertEqual(captured["name"], "reserve_stock")
        self.assertEqual(captured["args"], ("order-1",))
        self.assertEqual(captured["options"].task_queue, "orders")
        self.assertEqual(captured["options"].retry_policy.max_attempts, 3)
        self.assertEqual(captured["options"].timeout_seconds, 30)

    async def test_execute_activity_prefers_explicit_task_queue(self):
        registration = self._workflow_registration()
        captured: dict[str, object] = {}

        async def execute_activity(name, args, options):
            captured["name"] = name
            captured["args"] = args
            captured["options"] = options
            return options.task_queue

        with workflow._activate_execution_context(registration, execute_activity):
            result = await workflow.execute_activity(
                "charge_payment",
                args=("order-1",),
                task_queue="payment",
            )

        self.assertEqual(result, "payment")
        self.assertEqual(captured["options"].task_queue, "payment")


if __name__ == "__main__":
    unittest.main()
