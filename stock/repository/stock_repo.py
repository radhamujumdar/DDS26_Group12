import redis.asyncio as redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from models import DB_ERROR_STR, Reservation, ReservationState, StockValue


def reservation_key(tx_id: str, item_id: str) -> str:
    return f"txn:{tx_id}:{item_id}"



# Lua script for atomic prepare: eliminates WATCH/retry loops
# KEYS[1] = item_id key, KEYS[2] = reservation key
# ARGV[1] = amount, ARGV[2] = tx_id, ARGV[3] = item_id
# Returns: [status_code, detail]
PREPARE_LUA = """
local item_key = KEYS[1]
local rkey = KEYS[2]
local amount = tonumber(ARGV[1])
local tx_id = ARGV[2]
local item_id = ARGV[3]

local existing_raw = redis.call('GET', rkey)
if existing_raw then
    local existing = cmsgpack.unpack(existing_raw)
    if tonumber(existing['amount']) ~= amount then
        return {-1, 'prepare amount mismatch'}
    end
    if existing['state'] == 'prepared' then
        return {1, 'prepared'}
    end
    if existing['state'] == 'committed' then
        return {2, 'already committed'}
    end
    return {-1, 'previously aborted'}
end

local item_raw = redis.call('GET', item_key)
if not item_raw then
    return {-1, 'Item not found'}
end
local item = cmsgpack.unpack(item_raw)
if item['stock'] < amount then
    return {-1, 'insufficient stock'}
end

item['stock'] = item['stock'] - amount
redis.call('SET', item_key, cmsgpack.pack(item))
local reservation = {tx_id=tx_id, item_id=item_id, amount=amount, state='prepared'}
redis.call('SET', rkey, cmsgpack.pack(reservation))
return {0, 'prepared'}
"""

# Lua script for atomic commit
COMMIT_LUA = """
local rkey = KEYS[1]
local amount = tonumber(ARGV[1])

local raw = redis.call('GET', rkey)
if not raw then
    return {-1, 'Unknown transaction'}
end
local res = cmsgpack.unpack(raw)
if tonumber(res['amount']) ~= amount then
    return {-1, 'commit amount mismatch'}
end
if res['state'] == 'committed' then
    return {0, 'committed'}
end
if res['state'] == 'aborted' then
    return {-1, 'cannot commit, already aborted'}
end
if res['state'] ~= 'prepared' then
    return {-1, 'invalid state'}
end

res['state'] = 'committed'
redis.call('SET', rkey, cmsgpack.pack(res))
return {0, 'committed'}
"""

# Lua script for atomic abort
ABORT_LUA = """
local item_key = KEYS[1]
local rkey = KEYS[2]

local res_raw = redis.call('GET', rkey)
if not res_raw then
    return {0, 'aborted'}
end
local res = cmsgpack.unpack(res_raw)
if res['state'] == 'aborted' then
    return {0, 'aborted'}
end
if res['state'] == 'committed' then
    return {-1, 'cannot abort, already committed'}
end

local item_raw = redis.call('GET', item_key)
if item_raw then
    local item = cmsgpack.unpack(item_raw)
    item['stock'] = item['stock'] + tonumber(res['amount'])
    redis.call('SET', item_key, cmsgpack.pack(item))
end

res['state'] = 'aborted'
redis.call('SET', rkey, cmsgpack.pack(res))
return {0, 'aborted'}
"""


class StockRepository:
    def __init__(self, db: Redis):
        self.db = db
        self._prepare_script = db.register_script(PREPARE_LUA)
        self._commit_script = db.register_script(COMMIT_LUA)
        self._abort_script = db.register_script(ABORT_LUA)

    async def create_item(self, item_id: str, price: int):
        value = msgpack.encode(StockValue(stock=0, price=int(price)))
        try:
            await self.db.set(item_id, value)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def batch_init_items(self, kv_pairs: dict[str, bytes]):
        try:
            await self.db.mset(kv_pairs)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_item(self, item_id: str) -> StockValue | None:
        try:
            raw = await self.db.get(item_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return self._decode_stock(raw)

    async def get_item_or_error(self, item_id: str) -> StockValue:
        item = await self.get_item(item_id)
        if item is None:
            raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")
        return item

    async def save_item(self, item_id: str, item: StockValue):
        try:
            await self.db.set(item_id, msgpack.encode(item))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def add_stock_non_atomic(self, item_id: str, amount: int) -> int:
        item = await self.get_item_or_error(item_id)
        item.stock += int(amount)
        await self.save_item(item_id, item)
        return item.stock

    async def subtract_stock_non_atomic(self, item_id: str, amount: int) -> int:
        item = await self.get_item_or_error(item_id)
        item.stock -= int(amount)
        if item.stock < 0:
            raise HTTPException(status_code=400, detail=f"Item: {item_id} stock cannot get reduced below zero!")
        await self.save_item(item_id, item)
        return item.stock

    async def get_reservation(self, tx_id: str, item_id: str) -> Reservation | None:
        rkey = reservation_key(tx_id, item_id)
        try:
            raw = await self.db.get(rkey)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return self._decode_reservation(raw)

    async def save_reservation(self, reservation: Reservation):
        rkey = reservation_key(reservation.tx_id, reservation.item_id)
        try:
            await self.db.set(rkey, msgpack.encode(reservation))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def list_prepared_reservations(self) -> list[Reservation]:
        prepared: list[Reservation] = []
        try:
            async for raw_key in self.db.scan_iter(match="txn:*:*"):
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                if key.count(":") != 2:
                    continue
                raw_record = await self.db.get(key)
                if raw_record is None:
                    continue
                reservation = self._decode_reservation(raw_record)
                if reservation.state == ReservationState.PREPARED:
                    prepared.append(reservation)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return prepared

    async def prepare_reservation(self, tx_id: str, item_id: str, amount: int) -> str:
        rkey = reservation_key(tx_id, item_id)
        try:
            result = await self._prepare_script(
                keys=[item_id, rkey],
                args=[amount, tx_id, item_id],
            )
            status_code = int(result[0])
            detail = result[1].decode() if isinstance(result[1], bytes) else str(result[1])
            if status_code == 0 or status_code == 1:
                return ReservationState.PREPARED.value
            if status_code == 2:
                return "already committed"
            raise HTTPException(status_code=400, detail=f"Txn {tx_id}: {detail} for item {item_id}")
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def commit_reservation(self, tx_id: str, item_id: str, amount: int) -> str:
        rkey = reservation_key(tx_id, item_id)
        try:
            result = await self._commit_script(keys=[rkey], args=[amount])
            status_code = int(result[0])
            detail = result[1].decode() if isinstance(result[1], bytes) else str(result[1])
            if status_code == 0:
                return ReservationState.COMMITTED.value
            raise HTTPException(status_code=400, detail=f"Txn {tx_id}: {detail} for item {item_id}")
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def abort_reservation(self, tx_id: str, item_id: str, amount: int) -> str:
        rkey = reservation_key(tx_id, item_id)
        try:
            result = await self._abort_script(keys=[item_id, rkey], args=[])
            status_code = int(result[0])
            detail = result[1].decode() if isinstance(result[1], bytes) else str(result[1])
            if status_code == 0:
                return ReservationState.ABORTED.value
            raise HTTPException(status_code=400, detail=f"Txn {tx_id}: {detail} for item {item_id}")
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    @staticmethod
    def _decode_stock(raw: bytes) -> StockValue:
        try:
            return msgpack.decode(raw, type=StockValue)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    @staticmethod
    def _decode_reservation(raw: bytes) -> Reservation:
        try:
            return msgpack.decode(raw, type=Reservation)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
