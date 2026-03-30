"""Order-side Fluxi worker.

Handles the ReferenceCheckoutWorkflow and the two order-domain activities
(load_order, mark_order_paid). Runs as a separate process alongside the
order-service, polling the fluxi-server for tasks on the "orders" queue.

Required environment variables:
  REDIS_HOST / REDIS_PORT / REDIS_PASSWORD / REDIS_DB  — order-db connection
  FLUXI_REDIS_URL                                       — fluxi engine Redis
"""

from __future__ import annotations

import asyncio
import os

import redis
from msgspec import Struct, msgpack

from fluxi_sdk import activity
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.examples.checkout import CheckoutItem, CheckoutOrder, ReferenceCheckoutWorkflow
from fluxi_sdk.runtime.redis import RedisFluxiRuntime 
from fluxi_sdk.worker import Worker


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


_db = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


@activity.defn(name="load_order")
def load_order(order_id: str) -> CheckoutOrder:
    raw = _db.get(order_id)
    if raw is None:
        raise KeyError(f"Order {order_id!r} not found")
    entry: OrderValue = msgpack.decode(raw, type=OrderValue)
    return _to_checkout_order(order_id, entry)


@activity.defn(name="mark_order_paid")
def mark_order_paid(order_id: str) -> CheckoutOrder:
    raw = _db.get(order_id)
    if raw is None:
        raise KeyError(f"Order {order_id!r} not found")
    entry: OrderValue = msgpack.decode(raw, type=OrderValue)
    entry.paid = True
    _db.set(order_id, msgpack.encode(entry))
    return _to_checkout_order(order_id, entry)


def _to_checkout_order(order_id: str, entry: OrderValue) -> CheckoutOrder:
    quantities: dict[str, int] = {}
    for item_id, qty in entry.items:
        quantities[item_id] = quantities.get(item_id, 0) + qty
    items = tuple(CheckoutItem(item_id=k, quantity=v) for k, v in quantities.items())
    return CheckoutOrder(
        order_id=order_id,
        user_id=entry.user_id,
        total_cost=entry.total_cost,
        items=items,
        paid=entry.paid,
    )


async def main() -> None:
    runtime = RedisFluxiRuntime(redis_url=os.environ["FLUXI_REDIS_URL"])
    client = WorkflowClient.connect(runtime=runtime)
    worker = Worker(
        client,
        task_queue="orders",
        workflows=[ReferenceCheckoutWorkflow],
        activities=[load_order, mark_order_paid],
    )
    # run_forever() polls the fluxi-server for workflow and activity tasks
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
