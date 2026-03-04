import uuid

import redis
from fastapi import HTTPException
from msgspec import msgpack
from redis.asyncio import Redis

from models import DB_ERROR_STR, OrderValue


class OrderRepository:
    def __init__(self, db: Redis):
        self.db = db

    async def create_order(self, user_id: str) -> str:
        key = str(uuid.uuid4())
        value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
        try:
            await self.db.set(key, value)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return key

    async def save_order(self, order_id: str, order_entry: OrderValue):
        try:
            await self.db.set(order_id, msgpack.encode(order_entry))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def batch_set_orders(self, kv_pairs: dict[str, bytes]):
        try:
            await self.db.mset(kv_pairs)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_order(self, order_id: str) -> OrderValue:
        try:
            entry: bytes = await self.db.get(order_id)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

        order_entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
        if order_entry is None:
            raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!")
        return order_entry
