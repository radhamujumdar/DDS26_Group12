import asyncio
from datetime import timedelta
import unittest

import fluxi_sdk_test_support  # noqa: F401

from fluxi_sdk import Worker, activity, errors, workflow
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.testing import FakeFluxiRuntime
from fluxi_sdk.types import RetryPolicy, StartPolicy


@activity.defn(name="load_order")
def load_order_activity(order_id: str) -> dict:
    return {
        "order_id": order_id,
        "item_id": "item-1",
        "quantity": 2,
        "user_id": "user-1",
        "total": 20,
    }


@activity.defn(name="reserve_stock")
async def reserve_stock_activity(item_id: str, quantity: int) -> dict:
    await asyncio.sleep(0)
    return {"item_id": item_id, "reserved": quantity}


@activity.defn(name="charge_payment")
def charge_payment_activity(user_id: str, total: int) -> dict:
    return {"user_id": user_id, "charged": total}


@workflow.defn
class CheckoutWorkflow:

    @workflow.run
    async def run(self, order_id: str) -> dict:
        order = await workflow.execute_activity(load_order_activity, order_id)
        reservation = await workflow.execute_activity(
            reserve_stock_activity,
            order["item_id"],
            order["quantity"],
            task_queue="stock",
            retry_policy=RetryPolicy(max_attempts=3),
            schedule_to_close_timeout=timedelta(seconds=30),
        )
        payment = await workflow.execute_activity(
            charge_payment_activity,
            order["user_id"],
            order["total"],
            task_queue="payment",
        )
        return {
            "order_id": order["order_id"],
            "reservation": reservation,
            "payment": payment,
        }


@workflow.defn
class SlowWorkflow:

    @workflow.run
    async def run(self, value: str) -> str:
        await workflow.execute_activity("wait_for_signal", value)
        return f"done:{value}"


class TestFakeFluxiRuntime(unittest.IsolatedAsyncioTestCase):

    @staticmethod
    def _create_runtime() -> tuple[FakeFluxiRuntime, WorkflowClient]:
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)
        return runtime, client

    async def test_executes_workflow_and_records_activity_metadata(self):
        runtime, client = self._create_runtime()
        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[CheckoutWorkflow],
            activities=[load_order_activity],
        )
        stock_worker = Worker(
            client,
            task_queue="stock",
            activities=[reserve_stock_activity],
        )
        payment_worker = Worker(
            client,
            task_queue="payment",
            activities=[charge_payment_activity],
        )

        async with orders_worker, stock_worker, payment_worker:
            result = await client.execute_workflow(
                CheckoutWorkflow.run,
                "order-1",
                id="checkout:order-1",
                task_queue="orders",
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
        self.assertEqual(run.workflow_id, "checkout:order-1")
        self.assertEqual(run.task_queue, "orders")
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
        self.assertEqual(
            reserve_execution.options.schedule_to_close_timeout,
            timedelta(seconds=30),
        )
        self.assertEqual(
            reserve_execution.result,
            {"item_id": "item-1", "reserved": 2},
        )

        payment_execution = run.activity_executions[2]
        self.assertEqual(payment_execution.activity_name, "charge_payment")
        self.assertEqual(payment_execution.options.task_queue, "payment")

    async def test_executes_registered_workflow_by_name_string(self):
        runtime, client = self._create_runtime()
        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[CheckoutWorkflow],
            activities=[load_order_activity],
        )
        stock_worker = Worker(
            client,
            task_queue="stock",
            activities=[reserve_stock_activity],
        )
        payment_worker = Worker(
            client,
            task_queue="payment",
            activities=[charge_payment_activity],
        )

        async with orders_worker, stock_worker, payment_worker:
            result = await client.execute_workflow(
                "CheckoutWorkflow",
                "order-2",
                id="checkout:by-name",
                task_queue="orders",
            )

        self.assertEqual(result["order_id"], "order-2")
        self.assertEqual(len(runtime.workflow_runs), 1)
        self.assertEqual(runtime.workflow_runs[0].workflow_name, "CheckoutWorkflow")
        self.assertEqual(runtime.workflow_runs[0].workflow_id, "checkout:by-name")

    async def test_attach_or_start_reuses_existing_run_for_same_workflow_key(self):
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)
        signal = asyncio.Event()
        started = asyncio.Event()

        @activity.defn
        async def wait_for_signal(value: str) -> str:
            started.set()
            await signal.wait()
            return value

        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[SlowWorkflow],
            activities=[wait_for_signal],
        )

        async with orders_worker:
            first_task = asyncio.create_task(
                client.execute_workflow(
                    SlowWorkflow.run,
                    "alpha",
                    id="slow:1",
                    task_queue="orders",
                )
            )
            await started.wait()

            second_task = asyncio.create_task(
                client.execute_workflow(
                    SlowWorkflow.run,
                    "ignored",
                    id="slow:1",
                    task_queue="orders",
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

    async def test_attach_or_start_reuses_completed_run_for_same_workflow_key(self):
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)

        @activity.defn
        async def wait_for_signal(value: str) -> str:
            await asyncio.sleep(0)
            return value

        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[SlowWorkflow],
            activities=[wait_for_signal],
        )

        async with orders_worker:
            first_result = await client.execute_workflow(
                SlowWorkflow.run,
                "alpha",
                id="slow:completed",
                task_queue="orders",
            )
            second_result = await client.execute_workflow(
                SlowWorkflow.run,
                "beta",
                id="slow:completed",
                task_queue="orders",
            )

        self.assertEqual(first_result, "done:alpha")
        self.assertEqual(second_result, "done:alpha")
        self.assertEqual(len(runtime.workflow_runs), 1)
        self.assertEqual(runtime.workflow_runs[0].attach_count, 1)
        self.assertEqual(runtime.workflow_runs[0].args, ("alpha",))

    async def test_reject_duplicate_raises_for_existing_workflow_key(self):
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)
        signal = asyncio.Event()
        started = asyncio.Event()

        @activity.defn
        async def wait_for_signal(value: str) -> str:
            started.set()
            await signal.wait()
            return value

        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[SlowWorkflow],
            activities=[wait_for_signal],
        )

        async with orders_worker:
            first_task = asyncio.create_task(
                client.execute_workflow(
                    SlowWorkflow.run,
                    "alpha",
                    id="slow:reject",
                    task_queue="orders",
                )
            )
            await started.wait()

            with self.assertRaises(errors.WorkflowAlreadyStartedError):
                await client.execute_workflow(
                    SlowWorkflow.run,
                    "beta",
                    id="slow:reject",
                    task_queue="orders",
                    start_policy=StartPolicy.REJECT_DUPLICATE,
                )

            signal.set()
            self.assertEqual(await first_task, "done:alpha")
        self.assertEqual(len(runtime.workflow_runs), 1)

    async def test_allow_duplicate_starts_a_new_run(self):
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)

        @activity.defn
        async def wait_for_signal(value: str) -> str:
            await asyncio.sleep(0)
            return value

        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[SlowWorkflow],
            activities=[wait_for_signal],
        )

        async with orders_worker:
            first_result = await client.execute_workflow(
                SlowWorkflow.run,
                "alpha",
                id="slow:dup",
                task_queue="orders",
            )
            second_result = await client.execute_workflow(
                SlowWorkflow.run,
                "beta",
                id="slow:dup",
                task_queue="orders",
                start_policy=StartPolicy.ALLOW_DUPLICATE,
            )

        self.assertEqual(first_result, "done:alpha")
        self.assertEqual(second_result, "done:beta")
        self.assertEqual(len(runtime.workflow_runs), 2)
        self.assertEqual(
            [run.args for run in runtime.get_workflow_runs_for_id("slow:dup")],
            [("alpha",), ("beta",)],
        )

    async def test_requires_running_worker_for_workflow_task_queue(self):
        runtime, client = self._create_runtime()
        Worker(
            client,
            task_queue="orders",
            workflows=[CheckoutWorkflow],
            activities=[load_order_activity],
        )

        with self.assertRaises(errors.NoWorkflowWorkerAvailableError):
            await client.execute_workflow(
                CheckoutWorkflow.run,
                "missing",
                id="missing:1",
                task_queue="orders",
            )

    async def test_requires_running_worker_for_activity_task_queue(self):
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)

        @activity.defn(name="charge_payment")
        def charge_payment(user_id: str, amount: int) -> dict:
            return {"user_id": user_id, "charged": amount}

        @workflow.defn
        class MissingActivityWorkflow:

            @workflow.run
            async def run(self) -> None:
                await workflow.execute_activity(
                    charge_payment,
                    "user-1",
                    10,
                    task_queue="payment",
                )

        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[MissingActivityWorkflow],
        )
        Worker(
            client,
            task_queue="payment",
            activities=[charge_payment],
        )

        async with orders_worker:
            with self.assertRaises(errors.NoActivityWorkerAvailableError):
                await client.execute_workflow(
                    MissingActivityWorkflow.run,
                    id="missing-activity:1",
                    task_queue="orders",
                )

        run = runtime.workflow_runs[0]
        self.assertEqual(run.status, "failed")
        self.assertIsInstance(run.error, errors.NoActivityWorkerAvailableError)
        self.assertEqual(run.activity_executions[0].status, "failed")
        self.assertIsInstance(
            run.activity_executions[0].error,
            errors.NoActivityWorkerAvailableError,
        )


if __name__ == "__main__":
    unittest.main()
