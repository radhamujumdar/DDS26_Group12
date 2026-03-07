import asyncio
import time
import uuid

import redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis

from models import DB_ERROR_STR, SagaState, SagaTxRecord


class SagaTxRepository:
    TX_PREFIX = "saga:tx:"
    TX_ACTIVE = "saga:tx:active"
    TX_ORDER_PREFIX = "saga:tx:order:"
    TX_LOCK_PREFIX = "saga:tx:lock:"
    TX_ORDER_CREATE_LOCK_PREFIX = "saga:tx:order:create-lock:"

    def __init__(self, db: Redis):
        self.db = db

    def _tx_key(self, tx_id: str) -> str:
        return f"{self.TX_PREFIX}{tx_id}"

    def _tx_order_key(self, order_id: str) -> str:
        return f"{self.TX_ORDER_PREFIX}{order_id}"

    def _tx_lock_key(self, tx_id: str) -> str:
        return f"{self.TX_LOCK_PREFIX}{tx_id}"

    def _tx_order_create_lock_key(self, order_id: str) -> str:
        return f"{self.TX_ORDER_CREATE_LOCK_PREFIX}{order_id}"

    @staticmethod
    def _decode_str(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    async def create(
        self,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: SagaState = SagaState.INIT,
    ) -> SagaTxRecord:
        tx_id = str(uuid.uuid4())
        now = time.time()
        record = SagaTxRecord(
            tx_id=tx_id,
            order_id=order_id,
            user_id=user_id,
            total_cost=total_cost,
            items=items,
            state=state.value,
            created_at=now,
            updated_at=now,
        )
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                pipe.set(self._tx_key(record.tx_id), msgpack.encode(record))
                pipe.sadd(self.TX_ACTIVE, record.tx_id)
                pipe.set(self._tx_order_key(order_id), record.tx_id)
                await pipe.execute()
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return record

    async def get_or_create_by_order(
        self,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: SagaState = SagaState.INIT,
        wait_timeout_seconds: float = 2.0,
    ) -> tuple[SagaTxRecord, bool]:
        deadline = time.monotonic() + wait_timeout_seconds
        while True:
            existing = await self.get_by_order(order_id)
            if existing is not None:
                return existing, False

            locked = await self.acquire_order_create_lock(order_id)
            if locked:
                try:
                    existing = await self.get_by_order(order_id)
                    if existing is not None:
                        return existing, False
                    created = await self.create(
                        order_id=order_id,
                        user_id=user_id,
                        total_cost=total_cost,
                        items=items,
                        state=state,
                    )
                    return created, True
                finally:
                    await self.release_order_create_lock(order_id)

            if time.monotonic() >= deadline:
                raise HTTPException(
                    status_code=400,
                    detail=f"Timed out acquiring saga transaction-create lock for order {order_id}",
                )
            await asyncio.sleep(0.05)

    async def get(self, tx_id: str) -> SagaTxRecord | None:
        try:
            entry: bytes = await self.db.get(self._tx_key(tx_id))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

        if not entry:
            return None

        try:
            return msgpack.decode(entry, type=SagaTxRecord)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_by_order(self, order_id: str) -> SagaTxRecord | None:
        try:
            tx_id = await self.db.get(self._tx_order_key(order_id))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

        if tx_id is None:
            return None

        tx_id_str = tx_id.decode() if isinstance(tx_id, bytes) else str(tx_id)
        record = await self.get(tx_id_str)
        if record is None:
            await self.clear_order_tx(order_id)
        return record

    async def save(self, record: SagaTxRecord):
        try:
            await self.db.set(self._tx_key(record.tx_id), msgpack.encode(record))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def update(self, tx_id: str, **changes) -> SagaTxRecord:
        existing = await self.get(tx_id)
        if existing is None:
            raise HTTPException(status_code=400, detail=f"Saga transaction {tx_id} not found")

        updated = SagaTxRecord(
            tx_id=existing.tx_id,
            order_id=existing.order_id,
            user_id=changes.get("user_id", existing.user_id),
            total_cost=changes.get("total_cost", existing.total_cost),
            items=changes.get("items", existing.items),
            state=changes.get("state", existing.state),
            created_at=existing.created_at,
            updated_at=time.time(),
            stock_reserved_items=changes.get("stock_reserved_items", existing.stock_reserved_items),
            stock_released_items=changes.get("stock_released_items", existing.stock_released_items),
            payment_debited=changes.get("payment_debited", existing.payment_debited),
            payment_refunded=changes.get("payment_refunded", existing.payment_refunded),
            attempts=changes.get("attempts", existing.attempts),
            error=changes.get("error", existing.error),
        )
        await self.save(updated)
        return updated

    async def add_active(self, tx_id: str):
        try:
            await self.db.sadd(self.TX_ACTIVE, tx_id)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def remove_active(self, tx_id: str):
        try:
            await self.db.srem(self.TX_ACTIVE, tx_id)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def list_active(self) -> list[str]:
        try:
            tx_ids = await self.db.smembers(self.TX_ACTIVE)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return [tx_id.decode() if isinstance(tx_id, bytes) else str(tx_id) for tx_id in tx_ids]

    async def clear_order_tx(self, order_id: str):
        try:
            await self.db.delete(self._tx_order_key(order_id))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def acquire_tx_lock(self, tx_id: str, ttl_seconds: int = 30) -> str | None:
        token = str(uuid.uuid4())
        try:
            was_set = await self.db.set(self._tx_lock_key(tx_id), token, nx=True, ex=ttl_seconds)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return token if was_set else None

    async def renew_tx_lock(self, tx_id: str, token: str, ttl_seconds: int = 30) -> bool:
        lock_key = self._tx_lock_key(tx_id)
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(lock_key)
                        current = await pipe.get(lock_key)
                        if self._decode_str(current) != token:
                            await pipe.unwatch()
                            return False
                        pipe.multi()
                        pipe.expire(lock_key, ttl_seconds)
                        await pipe.execute()
                        return True
                    except redis.WatchError:
                        continue
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def release_tx_lock(self, tx_id: str, token: str) -> bool:
        lock_key = self._tx_lock_key(tx_id)
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(lock_key)
                        current = await pipe.get(lock_key)
                        if current is None:
                            await pipe.unwatch()
                            return False
                        if self._decode_str(current) != token:
                            await pipe.unwatch()
                            return False
                        pipe.multi()
                        pipe.delete(lock_key)
                        await pipe.execute()
                        return True
                    except redis.WatchError:
                        continue
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def acquire_order_create_lock(self, order_id: str, ttl_seconds: int = 10) -> bool:
        try:
            was_set = await self.db.set(self._tx_order_create_lock_key(order_id), "1", nx=True, ex=ttl_seconds)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return bool(was_set)

    async def release_order_create_lock(self, order_id: str):
        try:
            await self.db.delete(self._tx_order_create_lock_key(order_id))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
