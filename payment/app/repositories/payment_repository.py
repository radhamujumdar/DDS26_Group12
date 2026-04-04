from __future__ import annotations

import logging
import time
import uuid

from msgspec import msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from fluxi_engine.observability import elapsed_ms, trace_logging_enabled
from shop_common.checkout import PaymentReceipt
from shop_common.errors import DatabaseError
from shop_common.redis import (
    activity_dedupe_key,
    decode_dedupe_record,
    encode_error_record,
    encode_success_record,
    run_with_failover_retry,
)

from ..domain.errors import InsufficientCreditError, UserNotFoundError
from ..domain.models import UserValue


logger = logging.getLogger(__name__)
TRACE_LOGGING_ENABLED = trace_logging_enabled()


class PaymentRepository:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def _run_redis_operation(self, operation_name: str, operation):
        try:
            return await run_with_failover_retry(
                self._redis,
                operation,
                operation_name=operation_name,
                logger=logger,
                trace_logging_enabled=TRACE_LOGGING_ENABLED,
            )
        except RedisError as exc:
            raise DatabaseError("DB error") from exc

    async def create_user(self) -> str:
        user_id = str(uuid.uuid4())
        await self._run_redis_operation(
            "payment.create_user",
            lambda: self._redis.set(user_id, msgpack.encode(UserValue(credit=0))),
        )
        return user_id

    async def batch_init(self, *, count: int, starting_money: int) -> None:
        payload = {
            f"{index}": msgpack.encode(UserValue(credit=starting_money))
            for index in range(count)
        }
        await self._run_redis_operation(
            "payment.batch_init",
            lambda: self._redis.mset(payload),
        )

    async def get_user(self, user_id: str) -> UserValue:
        raw = await self._run_redis_operation(
            "payment.get_user",
            lambda: self._redis.get(user_id),
        )
        if raw is None:
            raise UserNotFoundError(f"User: {user_id} not found!")
        return msgpack.decode(raw, type=UserValue)

    async def add_funds(self, user_id: str, amount: int) -> UserValue:
        async def operation() -> UserValue:
            while True:
                try:
                    async with self._redis.pipeline(transaction=True) as pipe:
                        await pipe.watch(user_id)
                        raw = await pipe.get(user_id)
                        if raw is None:
                            raise UserNotFoundError(f"User: {user_id} not found!")

                        entry = msgpack.decode(raw, type=UserValue)
                        entry.credit += amount

                        pipe.multi()
                        pipe.set(user_id, msgpack.encode(entry))
                        await pipe.execute()
                        return entry
                except WatchError:
                    continue

        return await self._run_redis_operation("payment.add_funds", operation)

    async def pay(self, user_id: str, amount: int) -> UserValue:
        async def operation() -> UserValue:
            while True:
                try:
                    async with self._redis.pipeline(transaction=True) as pipe:
                        await pipe.watch(user_id)
                        raw = await pipe.get(user_id)
                        if raw is None:
                            raise UserNotFoundError(f"User: {user_id} not found!")

                        entry = msgpack.decode(raw, type=UserValue)
                        entry.credit -= amount
                        if entry.credit < 0:
                            raise InsufficientCreditError(
                                f"User: {user_id} credit cannot get reduced below zero!"
                            )

                        pipe.multi()
                        pipe.set(user_id, msgpack.encode(entry))
                        await pipe.execute()
                        return entry
                except WatchError:
                    continue

        return await self._run_redis_operation("payment.pay", operation)

    async def charge_payment_idempotent(
        self,
        user_id: str,
        *,
        amount: int,
        activity_execution_id: str,
    ) -> PaymentReceipt:
        dedupe_key = activity_dedupe_key(activity_execution_id)
        internal_not_found = f"User {user_id!r} not found"
        operation_start = time.perf_counter()
        watch_retries = 0

        async def operation() -> PaymentReceipt:
            nonlocal watch_retries

            while True:
                try:
                    async with self._redis.pipeline(transaction=True) as pipe:
                        await pipe.watch(dedupe_key, user_id)
                        existing = await pipe.get(dedupe_key)
                        if existing is not None:
                            if TRACE_LOGGING_ENABLED:
                                logger.info(
                                    "payment.charge.dedupe_hit user_id=%s amount=%d activity_execution_id=%s watch_retries=%d duration_ms=%.2f",
                                    user_id,
                                    amount,
                                    activity_execution_id,
                                    watch_retries,
                                    elapsed_ms(operation_start),
                                )
                            return self._materialize_charge_record(existing)

                        raw = await pipe.get(user_id)
                        if raw is None:
                            pipe.multi()
                            pipe.set(
                                dedupe_key,
                                encode_error_record("user_not_found", internal_not_found),
                            )
                            await pipe.execute()
                            if TRACE_LOGGING_ENABLED:
                                logger.info(
                                    "payment.charge.user_not_found user_id=%s amount=%d activity_execution_id=%s watch_retries=%d duration_ms=%.2f",
                                    user_id,
                                    amount,
                                    activity_execution_id,
                                    watch_retries,
                                    elapsed_ms(operation_start),
                                )
                            raise UserNotFoundError(internal_not_found)

                        entry = msgpack.decode(raw, type=UserValue)
                        if entry.credit < amount:
                            message = (
                                f"User {user_id!r} insufficient credit: need {amount}, have {entry.credit}"
                            )
                            pipe.multi()
                            pipe.set(
                                dedupe_key,
                                encode_error_record("insufficient_credit", message),
                            )
                            await pipe.execute()
                            if TRACE_LOGGING_ENABLED:
                                logger.info(
                                    "payment.charge.insufficient_credit user_id=%s amount=%d credit=%d activity_execution_id=%s watch_retries=%d duration_ms=%.2f",
                                    user_id,
                                    amount,
                                    entry.credit,
                                    activity_execution_id,
                                    watch_retries,
                                    elapsed_ms(operation_start),
                                )
                            raise InsufficientCreditError(message)

                        entry.credit -= amount
                        result = {
                            "payment_id": f"payment:{activity_execution_id}",
                            "user_id": user_id,
                            "amount": amount,
                        }
                        pipe.multi()
                        pipe.set(user_id, msgpack.encode(entry))
                        pipe.set(dedupe_key, encode_success_record(result))
                        await pipe.execute()
                        if TRACE_LOGGING_ENABLED:
                            logger.info(
                                "payment.charge.success user_id=%s amount=%d activity_execution_id=%s remaining_credit=%d watch_retries=%d duration_ms=%.2f",
                                user_id,
                                amount,
                                activity_execution_id,
                                entry.credit,
                                watch_retries,
                                elapsed_ms(operation_start),
                            )
                        return PaymentReceipt(
                            payment_id=str(result["payment_id"]),
                            user_id=user_id,
                            amount=amount,
                        )
                except WatchError:
                    watch_retries += 1
                    continue

        return await self._run_redis_operation("payment.charge_payment_idempotent", operation)

    @staticmethod
    def _materialize_charge_record(raw: bytes) -> PaymentReceipt:
        record = decode_dedupe_record(raw)
        if record.get("status") == "success":
            payload = record.get("result")
            if not isinstance(payload, dict):
                raise TypeError("Expected successful charge payload to be a dict.")
            return PaymentReceipt(
                payment_id=str(payload["payment_id"]),
                user_id=str(payload["user_id"]),
                amount=int(payload["amount"]),
            )

        error = record.get("error")
        if not isinstance(error, dict):
            raise TypeError("Expected error charge payload to be a dict.")
        code = str(error.get("code", "payment_error"))
        message = str(error.get("message", "Payment operation failed"))
        if code == "user_not_found":
            raise UserNotFoundError(message)
        raise InsufficientCreditError(message)
