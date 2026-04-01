from __future__ import annotations

import uuid

from msgspec import msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from shop_common.checkout import StockReservation
from shop_common.errors import DatabaseError
from shop_common.redis import (
    activity_dedupe_key,
    decode_dedupe_record,
    encode_error_record,
    encode_success_record,
)

from ..domain.errors import StockInsufficientError, StockItemNotFoundError
from ..domain.models import StockValue


class StockRepository:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def create_item(self, price: int) -> str:
        item_id = str(uuid.uuid4())
        value = msgpack.encode(StockValue(stock=0, price=price))
        try:
            await self._redis.set(item_id, value)
        except RedisError as exc:
            raise DatabaseError("DB error") from exc
        return item_id

    async def batch_init(self, *, count: int, starting_stock: int, item_price: int) -> None:
        payload = {
            f"{index}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
            for index in range(count)
        }
        try:
            await self._redis.mset(payload)
        except RedisError as exc:
            raise DatabaseError("DB error") from exc

    async def get_item(self, item_id: str) -> StockValue:
        try:
            raw = await self._redis.get(item_id)
        except RedisError as exc:
            raise DatabaseError("DB error") from exc
        if raw is None:
            raise StockItemNotFoundError(f"Item: {item_id} not found!")
        return msgpack.decode(raw, type=StockValue)

    async def add_stock(self, item_id: str, amount: int) -> StockValue:
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(item_id)
                    raw = await pipe.get(item_id)
                    if raw is None:
                        raise StockItemNotFoundError(f"Item: {item_id} not found!")

                    item = msgpack.decode(raw, type=StockValue)
                    item.stock += amount

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    await pipe.execute()
                    return item
            except WatchError:
                continue
            except RedisError as exc:
                raise DatabaseError("DB error") from exc

    async def subtract_stock(self, item_id: str, amount: int) -> StockValue:
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(item_id)
                    raw = await pipe.get(item_id)
                    if raw is None:
                        raise StockItemNotFoundError(f"Item: {item_id} not found!")

                    item = msgpack.decode(raw, type=StockValue)
                    item.stock -= amount
                    if item.stock < 0:
                        raise StockInsufficientError(
                            f"Item: {item_id} stock cannot get reduced below zero!"
                        )

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    await pipe.execute()
                    return item
            except WatchError:
                continue
            except RedisError as exc:
                raise DatabaseError("DB error") from exc

    async def reserve_stock_idempotent(
        self,
        item_id: str,
        *,
        quantity: int,
        activity_execution_id: str,
    ) -> StockReservation:
        dedupe_key = activity_dedupe_key(activity_execution_id)
        internal_not_found = f"Item {item_id!r} not found"

        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(dedupe_key, item_id)
                    existing = await pipe.get(dedupe_key)
                    if existing is not None:
                        return self._materialize_reservation_record(existing)

                    raw = await pipe.get(item_id)
                    if raw is None:
                        pipe.multi()
                        pipe.set(
                            dedupe_key,
                            encode_error_record("item_not_found", internal_not_found),
                        )
                        await pipe.execute()
                        raise StockItemNotFoundError(internal_not_found)

                    item = msgpack.decode(raw, type=StockValue)
                    if item.stock < quantity:
                        message = (
                            f"Item {item_id!r} insufficient stock: need {quantity}, have {item.stock}"
                        )
                        pipe.multi()
                        pipe.set(
                            dedupe_key,
                            encode_error_record("insufficient_stock", message),
                        )
                        await pipe.execute()
                        raise StockInsufficientError(message)

                    item.stock -= quantity
                    result = {"item_id": item_id, "quantity": quantity}
                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.set(dedupe_key, encode_success_record(result))
                    await pipe.execute()
                    return StockReservation(item_id=item_id, quantity=quantity)
            except WatchError:
                continue
            except RedisError as exc:
                raise DatabaseError("DB error") from exc

    async def release_stock_idempotent(
        self,
        item_id: str,
        *,
        quantity: int,
        activity_execution_id: str,
    ) -> StockReservation:
        dedupe_key = activity_dedupe_key(activity_execution_id)
        internal_not_found = f"Item {item_id!r} not found"

        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(dedupe_key, item_id)
                    existing = await pipe.get(dedupe_key)
                    if existing is not None:
                        return self._materialize_reservation_record(existing)

                    raw = await pipe.get(item_id)
                    if raw is None:
                        pipe.multi()
                        pipe.set(
                            dedupe_key,
                            encode_error_record("item_not_found", internal_not_found),
                        )
                        await pipe.execute()
                        raise StockItemNotFoundError(internal_not_found)

                    item = msgpack.decode(raw, type=StockValue)
                    item.stock += quantity
                    result = {"item_id": item_id, "quantity": quantity}
                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.set(dedupe_key, encode_success_record(result))
                    await pipe.execute()
                    return StockReservation(item_id=item_id, quantity=quantity)
            except WatchError:
                continue
            except RedisError as exc:
                raise DatabaseError("DB error") from exc

    @staticmethod
    def _materialize_reservation_record(raw: bytes) -> StockReservation:
        record = decode_dedupe_record(raw)
        if record.get("status") == "success":
            payload = record.get("result")
            if not isinstance(payload, dict):
                raise TypeError("Expected reservation result payload to be a dict.")
            return StockReservation(
                item_id=str(payload["item_id"]),
                quantity=int(payload["quantity"]),
            )

        error = record.get("error")
        if not isinstance(error, dict):
            raise TypeError("Expected reservation error payload to be a dict.")
        code = str(error.get("code", "stock_error"))
        message = str(error.get("message", "Stock operation failed"))
        if code == "item_not_found":
            raise StockItemNotFoundError(message)
        raise StockInsufficientError(message)
