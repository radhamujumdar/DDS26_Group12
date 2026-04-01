"""Payment-side Fluxi activity worker.

Handles the charge_payment activity by operating directly on payment-db.
Runs as a separate process alongside the payment-service, polling the
fluxi-server for tasks on the "payment" queue.

Required environment variables:
  REDIS_HOST / REDIS_PORT / REDIS_PASSWORD / REDIS_DB  — payment-db connection
  FLUXI_REDIS_URL                                       — fluxi engine Redis
"""

from __future__ import annotations

import asyncio
import os

import redis
from msgspec import Struct, msgpack

from fluxi_sdk import activity
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.client import EngineConnectionConfig
from fluxi_sdk.examples.checkout import PaymentDeclinedError, PaymentReceipt
from fluxi_sdk.worker import Worker


class UserValue(Struct):
    credit: int


_db = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ["REDIS_PORT"]),
    password=os.environ["REDIS_PASSWORD"],
    db=int(os.environ["REDIS_DB"]),
)


@activity.defn(name="charge_payment")
def charge_payment(user_id: str, amount: int) -> PaymentReceipt:
    raw = _db.get(user_id)
    if raw is None:
        raise PaymentDeclinedError(f"User {user_id!r} not found")
    entry: UserValue = msgpack.decode(raw, type=UserValue)
    if entry.credit < amount:
        raise PaymentDeclinedError(
            f"User {user_id!r} insufficient credit: need {amount}, have {entry.credit}"
        )
    entry.credit -= amount
    _db.set(user_id, msgpack.encode(entry))
    return PaymentReceipt(
        payment_id=f"payment:{user_id}:{amount}",
        user_id=user_id,
        amount=amount,
    )


async def main() -> None:
    engine = EngineConnectionConfig.from_env()
    client = WorkflowClient.connect(engine=engine)
    worker = Worker(
        client,
        task_queue="payment",
        activities=[charge_payment],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
