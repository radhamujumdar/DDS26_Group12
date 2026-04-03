from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Sequence

from msgspec import msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from fluxi_engine.observability import elapsed_ms, trace_logging_enabled
from shop_common.checkout import CheckoutItem, StockReservation
from shop_common.errors import DatabaseError
from shop_common.redis import (
    activity_dedupe_key,
    decode_dedupe_record,
    encode_error_record,
    encode_success_record,
)

from ..domain.errors import StockInsufficientError, StockItemNotFoundError
from ..domain.models import StockValue


logger = logging.getLogger(__name__)
TRACE_LOGGING_ENABLED = trace_logging_enabled()


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

    async def reserve_stock_batch_idempotent(
        self,
        items: Sequence[CheckoutItem],
        *,
        activity_execution_id: str,
    ) -> tuple[StockReservation, ...]:
        if not items:
            return ()

        dedupe_key = activity_dedupe_key(activity_execution_id)
        item_ids = [item.item_id for item in items]
        operation_start = time.perf_counter()
        watch_retries = 0
        total_quantity = sum(item.quantity for item in items)

        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(dedupe_key, *item_ids)
                    existing = await pipe.get(dedupe_key)
                    if existing is not None:
                        if TRACE_LOGGING_ENABLED:
                            logger.info(
                                "stock.reserve_batch.dedupe_hit activity_execution_id=%s item_count=%d total_quantity=%d watch_retries=%d duration_ms=%.2f",
                                activity_execution_id,
                                len(items),
                                total_quantity,
                                watch_retries,
                                elapsed_ms(operation_start),
                            )
                        return self._materialize_reservation_batch_record(existing)

                    raws = await pipe.mget(item_ids)
                    values: list[StockValue] = []
                    for checkout_item, raw in zip(items, raws, strict=True):
                        if raw is None:
                            message = f"Item {checkout_item.item_id!r} not found"
                            pipe.multi()
                            pipe.set(
                                dedupe_key,
                                encode_error_record("item_not_found", message),
                            )
                            await pipe.execute()
                            if TRACE_LOGGING_ENABLED:
                                logger.info(
                                    "stock.reserve_batch.item_not_found activity_execution_id=%s item_id=%s item_count=%d total_quantity=%d watch_retries=%d duration_ms=%.2f",
                                    activity_execution_id,
                                    checkout_item.item_id,
                                    len(items),
                                    total_quantity,
                                    watch_retries,
                                    elapsed_ms(operation_start),
                                )
                            raise StockItemNotFoundError(message)
                        item = msgpack.decode(raw, type=StockValue)
                        if item.stock < checkout_item.quantity:
                            message = (
                                f"Item {checkout_item.item_id!r} insufficient stock: "
                                f"need {checkout_item.quantity}, have {item.stock}"
                            )
                            pipe.multi()
                            pipe.set(
                                dedupe_key,
                                encode_error_record("insufficient_stock", message),
                            )
                            await pipe.execute()
                            if TRACE_LOGGING_ENABLED:
                                logger.info(
                                    "stock.reserve_batch.insufficient activity_execution_id=%s item_id=%s requested=%d available=%d item_count=%d total_quantity=%d watch_retries=%d duration_ms=%.2f",
                                    activity_execution_id,
                                    checkout_item.item_id,
                                    checkout_item.quantity,
                                    item.stock,
                                    len(items),
                                    total_quantity,
                                    watch_retries,
                                    elapsed_ms(operation_start),
                                )
                            raise StockInsufficientError(message)
                        values.append(item)

                    result: list[dict[str, object]] = []
                    pipe.multi()
                    for checkout_item, item in zip(items, values, strict=True):
                        item.stock -= checkout_item.quantity
                        pipe.set(checkout_item.item_id, msgpack.encode(item))
                        result.append(
                            {
                                "item_id": checkout_item.item_id,
                                "quantity": checkout_item.quantity,
                            }
                        )
                    pipe.set(dedupe_key, encode_success_record(result))
                    await pipe.execute()
                    if TRACE_LOGGING_ENABLED:
                        logger.info(
                            "stock.reserve_batch.success activity_execution_id=%s item_count=%d total_quantity=%d watch_retries=%d duration_ms=%.2f",
                            activity_execution_id,
                            len(items),
                            total_quantity,
                            watch_retries,
                            elapsed_ms(operation_start),
                        )
                    return tuple(
                        StockReservation(
                            item_id=entry["item_id"],
                            quantity=entry["quantity"],
                        )
                        for entry in result
                    )
            except WatchError:
                watch_retries += 1
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

    async def release_stock_batch_idempotent(
        self,
        reservations: Sequence[StockReservation],
        *,
        activity_execution_id: str,
    ) -> tuple[StockReservation, ...]:
        if not reservations:
            return ()

        dedupe_key = activity_dedupe_key(activity_execution_id)
        item_ids = [reservation.item_id for reservation in reservations]
        operation_start = time.perf_counter()
        watch_retries = 0
        total_quantity = sum(reservation.quantity for reservation in reservations)

        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(dedupe_key, *item_ids)
                    existing = await pipe.get(dedupe_key)
                    if existing is not None:
                        if TRACE_LOGGING_ENABLED:
                            logger.info(
                                "stock.release_batch.dedupe_hit activity_execution_id=%s item_count=%d total_quantity=%d watch_retries=%d duration_ms=%.2f",
                                activity_execution_id,
                                len(reservations),
                                total_quantity,
                                watch_retries,
                                elapsed_ms(operation_start),
                            )
                        return self._materialize_reservation_batch_record(existing)

                    raws = await pipe.mget(item_ids)
                    values: list[StockValue] = []
                    for reservation, raw in zip(reservations, raws, strict=True):
                        if raw is None:
                            message = f"Item {reservation.item_id!r} not found"
                            pipe.multi()
                            pipe.set(
                                dedupe_key,
                                encode_error_record("item_not_found", message),
                            )
                            await pipe.execute()
                            if TRACE_LOGGING_ENABLED:
                                logger.info(
                                    "stock.release_batch.item_not_found activity_execution_id=%s item_id=%s item_count=%d total_quantity=%d watch_retries=%d duration_ms=%.2f",
                                    activity_execution_id,
                                    reservation.item_id,
                                    len(reservations),
                                    total_quantity,
                                    watch_retries,
                                    elapsed_ms(operation_start),
                                )
                            raise StockItemNotFoundError(message)
                        values.append(msgpack.decode(raw, type=StockValue))

                    result: list[dict[str, object]] = []
                    pipe.multi()
                    for reservation, item in zip(reservations, values, strict=True):
                        item.stock += reservation.quantity
                        pipe.set(reservation.item_id, msgpack.encode(item))
                        result.append(
                            {
                                "item_id": reservation.item_id,
                                "quantity": reservation.quantity,
                            }
                        )
                    pipe.set(dedupe_key, encode_success_record(result))
                    await pipe.execute()
                    if TRACE_LOGGING_ENABLED:
                        logger.info(
                            "stock.release_batch.success activity_execution_id=%s item_count=%d total_quantity=%d watch_retries=%d duration_ms=%.2f",
                            activity_execution_id,
                            len(reservations),
                            total_quantity,
                            watch_retries,
                            elapsed_ms(operation_start),
                        )
                    return tuple(
                        StockReservation(
                            item_id=entry["item_id"],
                            quantity=entry["quantity"],
                        )
                        for entry in result
                    )
            except WatchError:
                watch_retries += 1
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

    @staticmethod
    def _materialize_reservation_batch_record(raw: bytes) -> tuple[StockReservation, ...]:
        record = decode_dedupe_record(raw)
        if record.get("status") == "success":
            payload = record.get("result")
            if not isinstance(payload, list):
                raise TypeError("Expected reservation batch result payload to be a list.")
            return tuple(
                StockReservation(
                    item_id=str(entry["item_id"]),
                    quantity=int(entry["quantity"]),
                )
                for entry in payload
            )

        error = record.get("error")
        if not isinstance(error, dict):
            raise TypeError("Expected reservation batch error payload to be a dict.")
        code = str(error.get("code", "stock_error"))
        message = str(error.get("message", "Stock batch operation failed"))
        if code == "item_not_found":
            raise StockItemNotFoundError(message)
        raise StockInsufficientError(message)
