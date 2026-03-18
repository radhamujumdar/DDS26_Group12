import time
import uuid

import redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis

from models import SagaState, SagaTxRecord

CREATE_SAGA_TX_BY_ORDER_SCRIPT = """
local order_key = KEYS[1]
local tx_key = KEYS[2]
local active_key = KEYS[3]
local existing = redis.call('GET', order_key)
if existing then
    return {existing, 0}
end
redis.call('SET', order_key, ARGV[1])
redis.call('SET', tx_key, ARGV[2])
redis.call('SADD', active_key, ARGV[1])
return {ARGV[1], 1}
"""


class SagaTxRepository:
    TX_PREFIX = "saga:tx:"
    TX_ACTIVE = "saga:tx:active"
    TX_ORDER_PREFIX = "saga:tx:order:"
    TX_LOCK_PREFIX = "saga:tx:lock:"

    def __init__(self, db: Redis):
        self.db = db

    @staticmethod
    def _db_error(detail: str) -> HTTPException:
        return HTTPException(status_code=400, detail=detail)

    def _tx_key(self, tx_id: str) -> str:
        return f"{self.TX_PREFIX}{tx_id}"

    def _tx_order_key(self, order_id: str) -> str:
        return f"{self.TX_ORDER_PREFIX}{order_id}"

    def _tx_lock_key(self, tx_id: str) -> str:
        return f"{self.TX_LOCK_PREFIX}{tx_id}"

    @staticmethod
    def _decode_str(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    def _decode_record_bytes(self, raw: bytes, tx_id: str) -> SagaTxRecord:
        try:
            return msgpack.decode(raw, type=SagaTxRecord)
        except DecodeError as exc:
            raise self._db_error(f"Failed to decode saga transaction {tx_id} from Redis") from exc

    def _build_record(
        self,
        tx_id: str,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: SagaState,
    ) -> SagaTxRecord:
        now = time.time()
        return SagaTxRecord(
            tx_id=tx_id,
            order_id=order_id,
            user_id=user_id,
            total_cost=total_cost,
            items=items,
            state=state.value,
            created_at=now,
            updated_at=now,
        )

    async def create(
        self,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: SagaState = SagaState.INIT,
    ) -> SagaTxRecord:
        record = self._build_record(
            tx_id=str(uuid.uuid4()),
            order_id=order_id,
            user_id=user_id,
            total_cost=total_cost,
            items=items,
            state=state,
        )
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                pipe.set(self._tx_key(record.tx_id), msgpack.encode(record))
                pipe.sadd(self.TX_ACTIVE, record.tx_id)
                pipe.set(self._tx_order_key(order_id), record.tx_id)
                await pipe.execute()
        except redis.exceptions.RedisError as exc:
            raise self._db_error(
                f"Failed to create saga transaction {record.tx_id} for order {order_id} in Redis"
            ) from exc
        return record

    async def get_or_create_by_order(
        self,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: SagaState = SagaState.INIT,
        wait_timeout_seconds: float = 60.0,
    ) -> tuple[SagaTxRecord, bool]:
        deadline = time.monotonic() + wait_timeout_seconds
        while True:
            proposed_tx_id = str(uuid.uuid4())
            proposed_record = self._build_record(
                tx_id=proposed_tx_id,
                order_id=order_id,
                user_id=user_id,
                total_cost=total_cost,
                items=items,
                state=state,
            )

            try:
                result = await self.db.eval(
                    CREATE_SAGA_TX_BY_ORDER_SCRIPT,
                    3,
                    self._tx_order_key(order_id),
                    self._tx_key(proposed_tx_id),
                    self.TX_ACTIVE,
                    proposed_tx_id,
                    msgpack.encode(proposed_record),
                )
            except redis.exceptions.RedisError as exc:
                raise self._db_error(
                    f"Failed to create or load a saga transaction for order {order_id} in Redis"
                ) from exc

            existing_tx_id = self._decode_str(result[0] if isinstance(result, (list, tuple)) and result else None)
            created = bool(result[1]) if isinstance(result, (list, tuple)) and len(result) > 1 else False
            if created:
                return proposed_record, True

            existing = await self.get(existing_tx_id)
            if existing is not None:
                return existing, False

            await self.clear_order_tx(order_id)
            if time.monotonic() >= deadline:
                raise HTTPException(
                    status_code=400,
                    detail=f"Timed out resolving saga transaction for order {order_id}",
                )

    async def get(self, tx_id: str) -> SagaTxRecord | None:
        try:
            entry: bytes = await self.db.get(self._tx_key(tx_id))
        except redis.exceptions.RedisError as exc:
            raise self._db_error(f"Failed to load saga transaction {tx_id} from Redis") from exc

        if not entry:
            return None

        return self._decode_record_bytes(entry, tx_id)

    async def get_by_order(self, order_id: str) -> SagaTxRecord | None:
        try:
            tx_id = await self.db.get(self._tx_order_key(order_id))
        except redis.exceptions.RedisError as exc:
            raise self._db_error(
                f"Failed to load the saga transaction mapping for order {order_id}"
            ) from exc

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
            raise self._db_error(f"Failed to save saga transaction {record.tx_id} in Redis") from exc

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
            raise self._db_error(
                f"Failed to add saga transaction {tx_id} to the active transaction set"
            ) from exc

    async def remove_active(self, tx_id: str):
        try:
            await self.db.srem(self.TX_ACTIVE, tx_id)
        except redis.exceptions.RedisError as exc:
            raise self._db_error(
                f"Failed to remove saga transaction {tx_id} from the active transaction set"
            ) from exc

    async def list_active(self) -> list[str]:
        try:
            tx_ids = await self.db.smembers(self.TX_ACTIVE)
        except redis.exceptions.RedisError as exc:
            raise self._db_error("Failed to list active saga transactions from Redis") from exc
        return [tx_id.decode() if isinstance(tx_id, bytes) else str(tx_id) for tx_id in tx_ids]

    async def clear_order_tx(self, order_id: str):
        try:
            await self.db.delete(self._tx_order_key(order_id))
        except redis.exceptions.RedisError as exc:
            raise self._db_error(
                f"Failed to clear the saga transaction mapping for order {order_id}"
            ) from exc

    async def acquire_tx_lock(self, tx_id: str, ttl_seconds: int = 30) -> str | None:
        token = str(uuid.uuid4())
        try:
            was_set = await self.db.set(self._tx_lock_key(tx_id), token, nx=True, ex=ttl_seconds)
        except redis.exceptions.RedisError as exc:
            raise self._db_error(f"Failed to acquire the lock for saga transaction {tx_id}") from exc
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
            raise self._db_error(f"Failed to renew the lock for saga transaction {tx_id}") from exc

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
            raise self._db_error(f"Failed to release the lock for saga transaction {tx_id}") from exc
