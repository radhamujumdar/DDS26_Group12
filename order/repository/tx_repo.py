import time
import uuid

import redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis

from models import DB_ERROR_STR, TxRecord, TxState

CREATE_TX_BY_ORDER_SCRIPT = """
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


class TxRepository:
    TX_PREFIX = "tx:"
    TX_ACTIVE = "tx:active"
    TX_ORDER_PREFIX = "tx:order:"
    TX_LOCK_PREFIX = "tx:lock:"

    def __init__(self, db: Redis):
        self.db = db

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

    def _build_record(
        self,
        tx_id: str,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: TxState,
    ) -> TxRecord:
        now = time.time()
        return TxRecord(
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
        state: TxState = TxState.INIT,
    ) -> TxRecord:
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
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return record

    async def get_or_create_by_order(
        self,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: TxState = TxState.INIT,
        wait_timeout_seconds: float = 2.0,
    ) -> tuple[TxRecord, bool]:
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
                    CREATE_TX_BY_ORDER_SCRIPT,
                    3,
                    self._tx_order_key(order_id),
                    self._tx_key(proposed_tx_id),
                    self.TX_ACTIVE,
                    proposed_tx_id,
                    msgpack.encode(proposed_record),
                )
            except redis.exceptions.RedisError as exc:
                raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

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
                    detail=f"Timed out resolving transaction for order {order_id}",
                )

    async def get(self, tx_id: str) -> TxRecord | None:
        try:
            entry: bytes = await self.db.get(self._tx_key(tx_id))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

        if not entry:
            return None

        try:
            return msgpack.decode(entry, type=TxRecord)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_by_order(self, order_id: str) -> TxRecord | None:
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

    async def save(self, record: TxRecord):
        try:
            await self.db.set(self._tx_key(record.tx_id), msgpack.encode(record))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def update(self, tx_id: str, **changes) -> TxRecord:
        existing = await self.get(tx_id)
        if existing is None:
            raise HTTPException(status_code=400, detail=f"Transaction {tx_id} not found")

        updated = TxRecord(
            tx_id=existing.tx_id,
            order_id=existing.order_id,
            user_id=changes.get("user_id", existing.user_id),
            total_cost=changes.get("total_cost", existing.total_cost),
            items=changes.get("items", existing.items),
            state=changes.get("state", existing.state),
            created_at=existing.created_at,
            updated_at=time.time(),
            stock_prepared_items=changes.get("stock_prepared_items", existing.stock_prepared_items),
            stock_committed_items=changes.get("stock_committed_items", existing.stock_committed_items),
            payment_prepared=changes.get("payment_prepared", existing.payment_prepared),
            payment_committed=changes.get("payment_committed", existing.payment_committed),
            attempts=changes.get("attempts", existing.attempts),
            error=changes.get("error", existing.error),
        )
        await self.save(updated)
        return updated

    async def update_state(self, tx_id: str, state: TxState, error: str | None = None):
        await self.update(tx_id, state=state.value, error=error)

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

    async def acquire_tx_lock(self, tx_id: str, ttl_seconds: int = 120) -> bool:
        try:
            was_set = await self.db.set(self._tx_lock_key(tx_id), "1", nx=True, ex=ttl_seconds)
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return bool(was_set)

    async def release_tx_lock(self, tx_id: str):
        try:
            await self.db.delete(self._tx_lock_key(tx_id))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
