"""Stock-side Fluxi activity worker.

Handles reserve_stock and release_stock activities by operating directly on
stock-db. Runs as a separate process alongside the stock-service, polling the
fluxi-server for tasks on the "stock" queue.

Required environment variables:
  REDIS_HOST / REDIS_PORT / REDIS_PASSWORD / REDIS_DB  — stock-db connection
  FLUXI_REDIS_URL                                       — fluxi engine Redis
"""

from __future__ import annotations

import asyncio
import os

import redis
from msgspec import Struct, msgpack

from fluxi_sdk import activity
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.examples.checkout import StockReservation, StockUnavailableError
from fluxi_sdk.runtime.redis import RedisFluxiRuntime
from fluxi_sdk.worker import Worker


class StockValue(Struct):
    stock: int
    price: int


_db = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


@activity.defn(name="reserve_stock")
def reserve_stock(item_id: str, quantity: int) -> StockReservation:
    raw = _db.get(item_id)
    if raw is None:
        raise StockUnavailableError(f"Item {item_id!r} not found")
    entry: StockValue = msgpack.decode(raw, type=StockValue)
    if entry.stock < quantity:
        raise StockUnavailableError(
            f"Item {item_id!r} insufficient stock: need {quantity}, have {entry.stock}"
        )
    entry.stock -= quantity
    _db.set(item_id, msgpack.encode(entry))
    return StockReservation(item_id=item_id, quantity=quantity)


@activity.defn(name="release_stock")
def release_stock(item_id: str, quantity: int) -> StockReservation:
    raw = _db.get(item_id)
    if raw is None:
        raise KeyError(f"Item {item_id!r} not found")
    entry: StockValue = msgpack.decode(raw, type=StockValue)
    entry.stock += quantity
    _db.set(item_id, msgpack.encode(entry))
    return StockReservation(item_id=item_id, quantity=quantity)


async def main() -> None:
    runtime = RedisFluxiRuntime(redis_url=os.environ["FLUXI_REDIS_URL"])
    client = WorkflowClient.connect(runtime=runtime)
    worker = Worker(
        client,
        task_queue="stock",
        activities=[reserve_stock, release_stock],
    )
    await worker.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
