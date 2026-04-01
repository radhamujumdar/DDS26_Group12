from __future__ import annotations

import uuid

from msgspec import msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from shop_common.checkout import PaymentReceipt
from shop_common.errors import DatabaseError
from shop_common.redis import (
    activity_dedupe_key,
    decode_dedupe_record,
    encode_error_record,
    encode_success_record,
)

from ..domain.errors import InsufficientCreditError, UserNotFoundError
from ..domain.models import UserValue


class PaymentRepository:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def create_user(self) -> str:
        user_id = str(uuid.uuid4())
        try:
            await self._redis.set(user_id, msgpack.encode(UserValue(credit=0)))
        except RedisError as exc:
            raise DatabaseError("DB error") from exc
        return user_id

    async def batch_init(self, *, count: int, starting_money: int) -> None:
        payload = {
            f"{index}": msgpack.encode(UserValue(credit=starting_money))
            for index in range(count)
        }
        try:
            await self._redis.mset(payload)
        except RedisError as exc:
            raise DatabaseError("DB error") from exc

    async def get_user(self, user_id: str) -> UserValue:
        try:
            raw = await self._redis.get(user_id)
        except RedisError as exc:
            raise DatabaseError("DB error") from exc
        if raw is None:
            raise UserNotFoundError(f"User: {user_id} not found!")
        return msgpack.decode(raw, type=UserValue)

    async def add_funds(self, user_id: str, amount: int) -> UserValue:
        entry = await self.get_user(user_id)
        entry.credit += amount
        try:
            await self._redis.set(user_id, msgpack.encode(entry))
        except RedisError as exc:
            raise DatabaseError("DB error") from exc
        return entry

    async def pay(self, user_id: str, amount: int) -> UserValue:
        entry = await self.get_user(user_id)
        entry.credit -= amount
        if entry.credit < 0:
            raise InsufficientCreditError(
                f"User: {user_id} credit cannot get reduced below zero!"
            )
        try:
            await self._redis.set(user_id, msgpack.encode(entry))
        except RedisError as exc:
            raise DatabaseError("DB error") from exc
        return entry

    async def charge_payment_idempotent(
        self,
        user_id: str,
        *,
        amount: int,
        activity_execution_id: str,
    ) -> PaymentReceipt:
        dedupe_key = activity_dedupe_key(activity_execution_id)
        internal_not_found = f"User {user_id!r} not found"

        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(dedupe_key, user_id)
                    existing = await pipe.get(dedupe_key)
                    if existing is not None:
                        return self._materialize_charge_record(existing)

                    raw = await pipe.get(user_id)
                    if raw is None:
                        pipe.multi()
                        pipe.set(
                            dedupe_key,
                            encode_error_record("user_not_found", internal_not_found),
                        )
                        await pipe.execute()
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
                    return PaymentReceipt(
                        payment_id=str(result["payment_id"]),
                        user_id=user_id,
                        amount=amount,
                    )
            except WatchError:
                continue
            except RedisError as exc:
                raise DatabaseError("DB error") from exc

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
