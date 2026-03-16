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

TX_TRANSITION_SCRIPT = """
local tx_key = KEYS[1]
local raw = redis.call('GET', tx_key)
if not raw then
    return {0, 'Transaction not found'}
end

local tx = cmsgpack.unpack(raw)
tx['state'] = ARGV[1]
tx['updated_at'] = tonumber(ARGV[2])
tx['attempts'] = tonumber(tx['attempts'] or 0) + tonumber(ARGV[3])

if ARGV[4] == '1' then
    tx['error'] = ARGV[5]
else
    tx['error'] = nil
end

if ARGV[6] == '1' then
    tx['stock_prepared_items'] = cmsgpack.unpack(ARGV[7])
end
if ARGV[8] == '1' then
    tx['stock_committed_items'] = cmsgpack.unpack(ARGV[9])
end
if ARGV[10] == '1' then
    tx['payment_prepared'] = ARGV[11] == '1'
end
if ARGV[12] == '1' then
    tx['payment_committed'] = ARGV[13] == '1'
end

local packed = cmsgpack.pack(tx)
redis.call('SET', tx_key, packed)
return {1, packed}
"""

TX_FINALIZE_COMMIT_SCRIPT = """
local tx_key = KEYS[1]
local active_key = KEYS[2]

local raw = redis.call('GET', tx_key)
if not raw then
    return {0, 'Transaction not found'}
end

local tx = cmsgpack.unpack(raw)
local order_key = tx['order_id']
local order_raw = redis.call('GET', order_key)
if not order_raw then
    return {-1, 'Order not found'}
end

local order_entry = cmsgpack.unpack(order_raw)
order_entry['paid'] = true

tx['state'] = ARGV[1]
tx['updated_at'] = tonumber(ARGV[2])
tx['attempts'] = tonumber(tx['attempts'] or 0) + tonumber(ARGV[3])
tx['stock_committed_items'] = cmsgpack.unpack(ARGV[4])
tx['payment_committed'] = ARGV[5] == '1'
tx['error'] = nil

local packed = cmsgpack.pack(tx)
redis.call('SET', tx_key, packed)
redis.call('SET', order_key, cmsgpack.pack(order_entry))
redis.call('SREM', active_key, tx['tx_id'])
return {1, packed}
"""

TX_FINALIZE_ABORT_SCRIPT = """
local tx_key = KEYS[1]
local active_key = KEYS[2]

local raw = redis.call('GET', tx_key)
if not raw then
    return {0, 'Transaction not found'}
end

local tx = cmsgpack.unpack(raw)
local order_key = ARGV[5] .. tx['order_id']
tx['state'] = ARGV[1]
tx['updated_at'] = tonumber(ARGV[2])
if ARGV[3] == '1' then
    tx['error'] = ARGV[4]
else
    tx['error'] = nil
end

local packed = cmsgpack.pack(tx)
redis.call('SET', tx_key, packed)
redis.call('SREM', active_key, tx['tx_id'])

local mapped_tx_id = redis.call('GET', order_key)
if mapped_tx_id and mapped_tx_id == tx['tx_id'] then
    redis.call('DEL', order_key)
end

return {1, packed}
"""


class TxRepository:
    TX_PREFIX = "tx:"
    TX_ACTIVE = "tx:active"
    TX_ORDER_PREFIX = "tx:order:"
    TX_LOCK_PREFIX = "tx:lock:"

    def __init__(self, db: Redis):
        self.db = db
        self._transition_script = db.register_script(TX_TRANSITION_SCRIPT)
        self._finalize_commit_script = db.register_script(TX_FINALIZE_COMMIT_SCRIPT)
        self._finalize_abort_script = db.register_script(TX_FINALIZE_ABORT_SCRIPT)

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

    @staticmethod
    def _encode_msgpack(value: object) -> bytes:
        return msgpack.encode(value)

    @staticmethod
    def _flag(value: bool) -> str:
        return "1" if value else "0"

    def _decode_record_bytes(self, raw: bytes) -> TxRecord:
        try:
            return msgpack.decode(raw, type=TxRecord)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    def _decode_transition_result(self, result, tx_id: str) -> TxRecord:
        if not isinstance(result, (list, tuple)) or len(result) < 2:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

        status_code = int(result[0])
        payload = result[1]
        if status_code != 1:
            detail = self._decode_str(payload) or f"Transaction {tx_id} not found"
            raise HTTPException(status_code=400, detail=detail)
        if not isinstance(payload, (bytes, bytearray)):
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)
        return self._decode_record_bytes(bytes(payload))

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

    async def persist_prepare_outcome(
        self,
        tx_id: str,
        stock_prepared_items: list[tuple[str, int]],
        payment_prepared: bool,
        attempts_increment: int,
        error: str | None = None,
    ) -> TxRecord:
        state = TxState.ABORTING if error else TxState.PREPARED
        updated_at = time.time()
        try:
            result = await self._transition_script(
                keys=[self._tx_key(tx_id)],
                args=[
                    state.value,
                    updated_at,
                    attempts_increment,
                    self._flag(error is not None),
                    error or "",
                    "1",
                    self._encode_msgpack(stock_prepared_items),
                    "0",
                    b"",
                    "1",
                    self._flag(payment_prepared),
                    "0",
                    "0",
                ],
            )
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return self._decode_transition_result(result, tx_id)

    async def persist_commit_progress(
        self,
        tx_id: str,
        stock_committed_items: list[tuple[str, int]],
        payment_committed: bool,
        attempts_increment: int,
        error: str,
    ) -> TxRecord:
        updated_at = time.time()
        try:
            result = await self._transition_script(
                keys=[self._tx_key(tx_id)],
                args=[
                    TxState.COMMITTING.value,
                    updated_at,
                    attempts_increment,
                    "1",
                    error,
                    "0",
                    b"",
                    "1",
                    self._encode_msgpack(stock_committed_items),
                    "0",
                    "0",
                    "1",
                    self._flag(payment_committed),
                ],
            )
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return self._decode_transition_result(result, tx_id)

    async def persist_abort_failure(self, tx_id: str, error: str) -> TxRecord:
        updated_at = time.time()
        try:
            result = await self._transition_script(
                keys=[self._tx_key(tx_id)],
                args=[
                    TxState.ABORTING.value,
                    updated_at,
                    0,
                    "1",
                    error,
                    "0",
                    b"",
                    "0",
                    b"",
                    "0",
                    "0",
                    "0",
                    "0",
                ],
            )
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return self._decode_transition_result(result, tx_id)

    async def finalize_commit(
        self,
        tx_id: str,
        stock_committed_items: list[tuple[str, int]],
        payment_committed: bool,
        attempts_increment: int,
    ) -> TxRecord:
        updated_at = time.time()
        try:
            result = await self._finalize_commit_script(
                keys=[self._tx_key(tx_id), self.TX_ACTIVE],
                args=[
                    TxState.COMMITTED.value,
                    updated_at,
                    attempts_increment,
                    self._encode_msgpack(stock_committed_items),
                    self._flag(payment_committed),
                ],
            )
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return self._decode_transition_result(result, tx_id)

    async def finalize_abort(self, tx_id: str, error: str | None = None) -> TxRecord:
        updated_at = time.time()
        try:
            result = await self._finalize_abort_script(
                keys=[self._tx_key(tx_id), self.TX_ACTIVE],
                args=[
                    TxState.ABORTED.value,
                    updated_at,
                    self._flag(error is not None),
                    error or "",
                    self.TX_ORDER_PREFIX,
                ],
            )
        except redis.exceptions.RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return self._decode_transition_result(result, tx_id)

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
