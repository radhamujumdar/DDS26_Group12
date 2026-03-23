import uuid

import redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis

from models import OrderValue


def _order_key(order_id: str) -> str:
    """Prefixed order key to avoid collision with stock/payment keys in shared cluster."""
    return f"ord:{{{order_id}}}"


class OrderRepository:
    def __init__(self, db: Redis):
        self.db = db

    @staticmethod
    def _db_error(detail: str) -> HTTPException:
        return HTTPException(status_code=400, detail=detail)

    async def create_order(self, user_id: str) -> str:
        key = str(uuid.uuid4())
        value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
        try:
            await self.db.set(_order_key(key), value)
        except redis.exceptions.RedisError as exc:
            raise self._db_error(f"Failed to create order for user {user_id} in Redis") from exc
        return key

    async def save_order(self, order_id: str, order_entry: OrderValue):
        try:
            await self.db.set(_order_key(order_id), msgpack.encode(order_entry))
        except redis.exceptions.RedisError as exc:
            raise self._db_error(f"Failed to save order {order_id} in Redis") from exc

    async def batch_set_orders(self, kv_pairs: dict[str, bytes]):
        try:
            pipe = self.db.pipeline(transaction=False)
            for k, v in kv_pairs.items():
                pipe.set(_order_key(k), v)
            await pipe.execute()
        except redis.exceptions.RedisError as exc:
            raise self._db_error(f"Failed to batch initialize {len(kv_pairs)} orders in Redis") from exc

    async def get_order(self, order_id: str) -> OrderValue:
        try:
            entry: bytes = await self.db.get(_order_key(order_id))
        except redis.exceptions.RedisError as exc:
            raise self._db_error(f"Failed to load order {order_id} from Redis") from exc

        try:
            order_entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
        except DecodeError as exc:
            raise self._db_error(f"Failed to decode order {order_id} from Redis") from exc
        if order_entry is None:
            raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!")
        return order_entry

    async def mark_paid(self, order_id: str):
        order_entry = await self.get_order(order_id)
        if order_entry.paid:
            return
        order_entry.paid = True
        await self.save_order(order_id, order_entry)
