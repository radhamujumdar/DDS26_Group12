import time
import uuid

import redis
from fastapi import HTTPException
from msgspec import msgpack
from redis.asyncio import Redis

from models import DB_ERROR_STR, TxRecord, TxState


class TxRepository:
    TX_PREFIX = "tx:"
    TX_ACTIVE = "tx:active"

    def __init__(self, db: Redis):
        self.db = db

    def _tx_key(self, tx_id: str) -> str:
        return f"{self.TX_PREFIX}{tx_id}"

    async def create(self, order_id: str, state: TxState = TxState.INIT) -> TxRecord:
        tx_id = str(uuid.uuid4())
        now = time.time()
        record = TxRecord(
            tx_id=tx_id,
            order_id=order_id,
            state=state.value,
            created_at=now,
            updated_at=now,
        )
        await self.save(record)
        await self.add_active(tx_id)
        return record

    async def get(self, tx_id: str) -> TxRecord | None:
        try:
            entry: bytes = await self.db.get(self._tx_key(tx_id))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return msgpack.decode(entry, type=TxRecord) if entry else None

    async def save(self, record: TxRecord):
        try:
            await self.db.set(self._tx_key(record.tx_id), msgpack.encode(record))
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def update_state(self, tx_id: str, state: TxState, error: str | None = None):
        existing = await self.get(tx_id)
        if existing is None:
            return
        updated = TxRecord(
            tx_id=existing.tx_id,
            order_id=existing.order_id,
            state=state.value,
            created_at=existing.created_at,
            updated_at=time.time(),
            error=error,
        )
        await self.save(updated)

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
