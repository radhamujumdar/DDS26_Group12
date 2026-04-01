from __future__ import annotations

import random
import uuid

from msgspec import msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from shop_common.checkout import CheckoutOrder, OrderNotFoundError, coalesce_order_items
from shop_common.errors import DatabaseError
from shop_common.redis import (
    activity_dedupe_key,
    decode_dedupe_record,
    encode_error_record,
    encode_success_record,
)

from ..domain.models import OrderValue


class OrderRepository:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def create_order(self, user_id: str) -> str:
        order_id = str(uuid.uuid4())
        await self.save_order(
            order_id,
            OrderValue(paid=False, items=[], user_id=user_id, total_cost=0),
        )
        return order_id

    async def batch_init(
        self,
        *,
        count: int,
        n_items: int,
        n_users: int,
        item_price: int,
    ) -> None:
        def generate_entry() -> OrderValue:
            user_id = random.randint(0, n_users - 1)
            item1_id = random.randint(0, n_items - 1)
            item2_id = random.randint(0, n_items - 1)
            return OrderValue(
                paid=False,
                items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                user_id=f"{user_id}",
                total_cost=2 * item_price,
            )

        kv_pairs = {
            f"{index}": msgpack.encode(generate_entry())
            for index in range(count)
        }
        try:
            await self._redis.mset(kv_pairs)
        except RedisError as exc:
            raise DatabaseError("DB error") from exc

    async def get_order(self, order_id: str) -> OrderValue:
        try:
            raw = await self._redis.get(order_id)
        except RedisError as exc:
            raise DatabaseError("DB error") from exc
        if raw is None:
            raise OrderNotFoundError(f"Order {order_id!r} not found")
        return msgpack.decode(raw, type=OrderValue)

    async def save_order(self, order_id: str, order_entry: OrderValue) -> None:
        try:
            await self._redis.set(order_id, msgpack.encode(order_entry))
        except RedisError as exc:
            raise DatabaseError("DB error") from exc

    async def add_item(
        self,
        order_id: str,
        *,
        item_id: str,
        quantity: int,
        item_price: int,
    ) -> OrderValue:
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(order_id)
                    raw = await pipe.get(order_id)
                    if raw is None:
                        raise OrderNotFoundError(f"Order {order_id!r} not found")

                    order_entry = msgpack.decode(raw, type=OrderValue)
                    order_entry.items.append((item_id, quantity))
                    order_entry.total_cost += quantity * item_price

                    pipe.multi()
                    pipe.set(order_id, msgpack.encode(order_entry))
                    await pipe.execute()
                    return order_entry
            except WatchError:
                continue
            except RedisError as exc:
                raise DatabaseError("DB error") from exc

    async def load_checkout_order(self, order_id: str) -> CheckoutOrder:
        entry = await self.get_order(order_id)
        return self._to_checkout_order(order_id, entry)

    async def mark_order_paid_idempotent(
        self,
        order_id: str,
        *,
        activity_execution_id: str,
    ) -> CheckoutOrder:
        dedupe_key = activity_dedupe_key(activity_execution_id)

        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(dedupe_key, order_id)
                    existing = await pipe.get(dedupe_key)
                    if existing is not None:
                        return self._materialize_mark_paid_record(existing)

                    raw = await pipe.get(order_id)
                    if raw is None:
                        record = encode_error_record(
                            "order_not_found",
                            f"Order {order_id!r} not found",
                        )
                        pipe.multi()
                        pipe.set(dedupe_key, record)
                        await pipe.execute()
                        raise OrderNotFoundError(f"Order {order_id!r} not found")

                    entry = msgpack.decode(raw, type=OrderValue)
                    entry.paid = True
                    result = self._encode_checkout_order_result(order_id, entry)

                    pipe.multi()
                    pipe.set(order_id, msgpack.encode(entry))
                    pipe.set(dedupe_key, encode_success_record(result))
                    await pipe.execute()
                    return self._decode_checkout_order_result(result)
            except WatchError:
                continue
            except RedisError as exc:
                raise DatabaseError("DB error") from exc

    @staticmethod
    def _to_checkout_order(order_id: str, entry: OrderValue) -> CheckoutOrder:
        return CheckoutOrder(
            order_id=order_id,
            user_id=entry.user_id,
            total_cost=entry.total_cost,
            items=coalesce_order_items(entry.items),
            paid=entry.paid,
        )

    @classmethod
    def _encode_checkout_order_result(
        cls,
        order_id: str,
        entry: OrderValue,
    ) -> dict[str, object]:
        return {
            "order_id": order_id,
            "user_id": entry.user_id,
            "total_cost": entry.total_cost,
            "paid": entry.paid,
            "items": [
                {"item_id": item.item_id, "quantity": item.quantity}
                for item in coalesce_order_items(entry.items)
            ],
        }

    @staticmethod
    def _decode_checkout_order_result(payload: dict[str, object]) -> CheckoutOrder:
        items = tuple(
            {
                "item_id": str(item["item_id"]),
                "quantity": int(item["quantity"]),
            }
            for item in payload["items"]  # type: ignore[index]
        )
        return CheckoutOrder(
            order_id=str(payload["order_id"]),
            user_id=str(payload["user_id"]),
            total_cost=int(payload["total_cost"]),
            items=coalesce_order_items(
                (item["item_id"], item["quantity"]) for item in items
            ),
            paid=bool(payload["paid"]),
        )

    @classmethod
    def _materialize_mark_paid_record(cls, raw: bytes) -> CheckoutOrder:
        record = decode_dedupe_record(raw)
        if record.get("status") == "success":
            payload = record.get("result")
            if not isinstance(payload, dict):
                raise TypeError("Expected successful mark-paid record payload to be a dict.")
            return cls._decode_checkout_order_result(payload)

        error = record.get("error")
        if not isinstance(error, dict):
            raise TypeError("Expected error record payload to be a dict.")
        raise OrderNotFoundError(str(error.get("message", "Order not found")))
