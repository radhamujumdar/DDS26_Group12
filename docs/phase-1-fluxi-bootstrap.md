# Fluxi Phase 1 Bootstrap

Phase 1 gives you a Temporal-shaped SDK surface backed by the in-memory fake runtime. You can define workflows and activities against the public `fluxi_sdk` API now, then keep the same code shape when the real engine arrives later.

## Define a workflow and activities

```python
from datetime import timedelta

from fluxi_sdk import activity, workflow


@activity.defn
def reserve_stock(item_id: str, quantity: int) -> dict[str, int]:
    return {"item_id": item_id, "reserved": quantity}


@activity.defn
def charge_payment(user_id: str, amount: int) -> dict[str, int]:
    return {"user_id": user_id, "charged": amount}


@workflow.defn
class CheckoutWorkflow:
    @workflow.run
    async def run(
        self,
        order_id: str,
        item_id: str,
        user_id: str,
        total_cost: int,
    ) -> dict[str, object]:
        reservation = await workflow.execute_activity(
            reserve_stock,
            item_id,
            1,
            task_queue="stock",
            schedule_to_close_timeout=timedelta(seconds=30),
        )
        payment = await workflow.execute_activity(
            charge_payment,
            user_id,
            total_cost,
            task_queue="payment",
            schedule_to_close_timeout=timedelta(seconds=30),
        )
        return {"reservation": reservation, "payment": payment}
```

Notes:

- Prefer positional workflow and activity arguments.
- Prefer `id=` and `task_queue=` on `client.execute_workflow(...)`.
- Prefer `schedule_to_close_timeout=timedelta(...)` on `workflow.execute_activity(...)`.
- `workflow.execute_activity(...)` can take either a decorated activity callable or a registered activity name string.
- If `task_queue` is omitted when scheduling an activity, Fluxi uses the current workflow task queue.

## Start the fake runtime

```python
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.testing import FakeFluxiRuntime
from fluxi_sdk.worker import Worker


runtime = FakeFluxiRuntime()
client = WorkflowClient.connect(runtime=runtime)

orders_worker = Worker(
    client,
    task_queue="orders",
    workflows=[CheckoutWorkflow],
)
stock_worker = Worker(
    client,
    task_queue="stock",
    activities=[reserve_stock],
)
payment_worker = Worker(
    client,
    task_queue="payment",
    activities=[charge_payment],
)
```

`Worker(...)` is the public registration mechanism in phase 1. A workflow only starts if there is a running worker for its `task_queue` with that workflow registered. An activity only runs if there is a running worker for the scheduled activity `task_queue` with that activity registered.

## Execute a workflow

```python
async with orders_worker, stock_worker, payment_worker:
    result = await client.execute_workflow(
        CheckoutWorkflow.run,
        "order-123",
        "item-123",
        "user-123",
        20,
        id="checkout:order-123",
        task_queue="orders",
    )
```

The phase-1 fake runtime executes immediately in-process, but it still records workflow runs, scheduled activities, task queues, retry options, and timeouts for tests.

## Temporal-style compatibility helpers

`workflow.unsafe.imports_passed_through()` exists as a no-op compatibility context manager for Temporal-shaped source files. It does not provide sandbox or replay semantics in phase 1.

## Reference code in this repo

- SDK example: `packages/fluxi-sdk/src/fluxi_sdk/examples/checkout.py`
- Order-service seam: `order/app/services/checkout_service.py`
- SDK contract tests: `test/test_fluxi_sdk_fake_runtime.py`

## Intentionally missing until phase 2

- Redis-backed queues and durable workflow state
- Worker polling, leases, and multi-process execution
- Replay and sandbox semantics
- Enforced retries and enforced timeouts
- Real task-queue delivery across services

Transitional aliases still exist for now:

- `workflow_key=` as an alias for `id=`
- `args=` as an alias for positional workflow and activity arguments
- `timeout_seconds=` as an alias for `schedule_to_close_timeout=`

New code should prefer the Temporal-aligned names.
