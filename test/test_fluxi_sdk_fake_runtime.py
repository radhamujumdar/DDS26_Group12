import asyncio
import unittest

from fluxi_sdk import activity, errors, workflow
from fluxi_sdk.testing import FakeFluxiRuntime
from fluxi_sdk.types import RetryPolicy, StartPolicy


@workflow.defn(default_task_queue="orders")
class CheckoutWorkflow:

    @workflow.run
    async def run(self, order_id: str) -> dict:
        order = await workflow.execute_activity("load_order", args=(order_id,))
        reservation = await workflow.execute_activity(
            "reserve_stock",
            args=(order["item_id"], order["quantity"]),
            task_queue="stock",
            retry_policy=RetryPolicy(max_attempts=3),
            timeout_seconds=30,
        )
        payment = await workflow.execute_activity(
            "charge_payment",
            args=(order["user_id"], order["total"]),
            task_queue="payment",
        )
        return {
            "order_id": order["order_id"],
            "reservation": reservation,
            "payment": payment,
        }


@workflow.defn(default_task_queue="orders")
class SlowWorkflow:

    @workflow.run
    async def run(self, value: str) -> str:
        await workflow.execute_activity("wait_for_signal", args=(value,))
        return f"done:{value}"


class TestFakeFluxiRuntime(unittest.IsolatedAsyncioTestCase):

    def _create_runtime(self) -> FakeFluxiRuntime:
        runtime = FakeFluxiRuntime()
        runtime.register_workflow(CheckoutWorkflow)
        return runtime

    async def test_executes_workflow_and_records_activity_metadata(self):
        runtime = self._create_runtime()
        client = runtime.create_client()

        @activity.defn
        def load_order(order_id: str) -> dict:
            return {
                "order_id": order_id,
                "item_id": "item-1",
                "quantity": 2,
                "user_id": "user-1",
                "total": 20,
            }

        @activity.defn(name="reserve_stock")
        async def reserve_stock(item_id: str, quantity: int) -> dict:
            await asyncio.sleep(0)
            return {"item_id": item_id, "reserved": quantity}

        @activity.defn
        def charge_payment(user_id: str, total: int) -> dict:
            return {"user_id": user_id, "charged": total}

        runtime.register_activity(load_order)
        runtime.register_activity(reserve_stock)
        runtime.register_activity(charge_payment)

        result = await client.execute_workflow(
            CheckoutWorkflow,
            workflow_key="checkout:order-1",
            args=("order-1",),
        )

        self.assertEqual(
            result,
            {
                "order_id": "order-1",
                "reservation": {"item_id": "item-1", "reserved": 2},
                "payment": {"user_id": "user-1", "charged": 20},
            },
        )

        run = runtime.workflow_runs[0]
        self.assertEqual(run.workflow_key, "checkout:order-1")
        self.assertEqual(run.status, "completed")
        self.assertEqual(run.result, result)
        self.assertEqual(len(run.activity_executions), 3)

        load_order_execution = run.activity_executions[0]
        self.assertEqual(load_order_execution.activity_name, "load_order")
        self.assertEqual(load_order_execution.options.task_queue, "orders")
        self.assertEqual(load_order_execution.args, ("order-1",))

        reserve_execution = run.activity_executions[1]
        self.assertEqual(reserve_execution.activity_name, "reserve_stock")
        self.assertEqual(reserve_execution.options.task_queue, "stock")
        self.assertEqual(reserve_execution.options.retry_policy.max_attempts, 3)
        self.assertEqual(reserve_execution.options.timeout_seconds, 30)
        self.assertEqual(
            reserve_execution.result,
            {"item_id": "item-1", "reserved": 2},
        )

        payment_execution = run.activity_executions[2]
        self.assertEqual(payment_execution.activity_name, "charge_payment")
        self.assertEqual(payment_execution.options.task_queue, "payment")

    async def test_attach_or_start_reuses_existing_run_for_same_workflow_key(self):
        runtime = FakeFluxiRuntime()
        runtime.register_workflow(SlowWorkflow)
        signal = asyncio.Event()
        started = asyncio.Event()

        @activity.defn
        async def wait_for_signal(value: str) -> str:
            started.set()
            await signal.wait()
            return value

        runtime.register_activity(wait_for_signal)
        client = runtime.create_client()

        first_task = asyncio.create_task(
            client.execute_workflow(
                SlowWorkflow,
                workflow_key="slow:1",
                args=("alpha",),
            )
        )
        await started.wait()

        second_task = asyncio.create_task(
            client.execute_workflow(
                SlowWorkflow,
                workflow_key="slow:1",
                args=("ignored",),
            )
        )

        await asyncio.sleep(0)
        signal.set()

        first_result = await first_task
        second_result = await second_task

        self.assertEqual(first_result, "done:alpha")
        self.assertEqual(second_result, "done:alpha")
        self.assertEqual(len(runtime.workflow_runs), 1)
        self.assertEqual(runtime.workflow_runs[0].attach_count, 1)
        self.assertEqual(runtime.workflow_runs[0].args, ("alpha",))

    async def test_reject_duplicate_raises_for_existing_workflow_key(self):
        runtime = FakeFluxiRuntime()
        runtime.register_workflow(SlowWorkflow)
        signal = asyncio.Event()
        started = asyncio.Event()

        @activity.defn
        async def wait_for_signal(value: str) -> str:
            started.set()
            await signal.wait()
            return value

        runtime.register_activity(wait_for_signal)
        client = runtime.create_client()

        first_task = asyncio.create_task(
            client.execute_workflow(
                SlowWorkflow,
                workflow_key="slow:reject",
                args=("alpha",),
            )
        )
        await started.wait()

        with self.assertRaises(errors.WorkflowAlreadyStartedError):
            await client.execute_workflow(
                SlowWorkflow,
                workflow_key="slow:reject",
                args=("beta",),
                start_policy=StartPolicy.REJECT_DUPLICATE,
            )

        signal.set()
        self.assertEqual(await first_task, "done:alpha")
        self.assertEqual(len(runtime.workflow_runs), 1)

    async def test_allow_duplicate_starts_a_new_run(self):
        runtime = FakeFluxiRuntime()
        runtime.register_workflow(SlowWorkflow)

        @activity.defn
        async def wait_for_signal(value: str) -> str:
            await asyncio.sleep(0)
            return value

        runtime.register_activity(wait_for_signal)
        client = runtime.create_client()

        first_result = await client.execute_workflow(
            SlowWorkflow,
            workflow_key="slow:dup",
            args=("alpha",),
        )
        second_result = await client.execute_workflow(
            SlowWorkflow,
            workflow_key="slow:dup",
            args=("beta",),
            start_policy=StartPolicy.ALLOW_DUPLICATE,
        )

        self.assertEqual(first_result, "done:alpha")
        self.assertEqual(second_result, "done:beta")
        self.assertEqual(len(runtime.workflow_runs), 2)
        self.assertEqual(
            [run.args for run in runtime.get_workflow_runs_for_key("slow:dup")],
            [("alpha",), ("beta",)],
        )

    async def test_unknown_workflow_raises(self):
        runtime = FakeFluxiRuntime()
        client = runtime.create_client()

        with self.assertRaises(errors.UnknownWorkflowError):
            await client.execute_workflow(
                "missing-workflow",
                workflow_key="missing:1",
            )

    async def test_unknown_activity_fails_workflow_and_is_recorded(self):
        runtime = FakeFluxiRuntime()

        @workflow.defn(default_task_queue="orders")
        class MissingActivityWorkflow:

            @workflow.run
            async def run(self) -> None:
                await workflow.execute_activity("missing-activity")

        runtime.register_workflow(MissingActivityWorkflow)
        client = runtime.create_client()

        with self.assertRaises(errors.UnknownActivityError):
            await client.execute_workflow(
                MissingActivityWorkflow,
                workflow_key="missing-activity:1",
            )

        run = runtime.workflow_runs[0]
        self.assertEqual(run.status, "failed")
        self.assertIsInstance(run.error, errors.UnknownActivityError)
        self.assertEqual(run.activity_executions[0].status, "failed")
        self.assertIsInstance(
            run.activity_executions[0].error,
            errors.UnknownActivityError,
        )


if __name__ == "__main__":
    unittest.main()
