import redis.asyncio as redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError

from models import DB_ERROR_STR, PrepareRecord, SagaDebitRecord, SagaDebitState, TxnState, UserValue


def txn_key(txn_id: str) -> str:
    return f"txn:{txn_id}"


def prepared_user_key(user_id: str) -> str:
    return f"txn:user:{user_id}:prepared"



# Lua script for atomic prepare_transaction (uses msgpack maps)
# KEYS[1] = user_id key, KEYS[2] = txn_key, KEYS[3] = prepared_user_key
# ARGV[1] = txn_id, ARGV[2] = user_id, ARGV[3] = amount
PAYMENT_PREPARE_LUA = """
local user_key = KEYS[1]
local tx_key = KEYS[2]
local prep_user_key = KEYS[3]
local txn_id = ARGV[1]
local user_id = ARGV[2]
local amount = tonumber(ARGV[3])

local existing_raw = redis.call('GET', tx_key)
if existing_raw then
    local existing = cmsgpack.unpack(existing_raw)
    if existing['user_id'] ~= user_id or tonumber(existing['delta']) ~= -amount then
        return {-1, 'Transaction parameters mismatch'}
    end
    if existing['state'] == 'aborted' then
        return {-2, 'Insufficient credit for user ' .. user_id}
    end
    return {1, existing_raw}
end

local user_raw = redis.call('GET', user_key)
if not user_raw then
    return {-1, 'User not found'}
end
local user = cmsgpack.unpack(user_raw)
local credit = user['credit']

if credit < amount then
    local rec = {txn_id=txn_id, user_id=user_id, delta=-amount, old_credit=credit, new_credit=credit, state='aborted'}
    redis.call('SET', tx_key, cmsgpack.pack(rec))
    return {-2, 'Insufficient credit for user ' .. user_id}
end

local new_credit = credit - amount
local rec = {txn_id=txn_id, user_id=user_id, delta=-amount, old_credit=credit, new_credit=new_credit, state='prepared'}
redis.call('SET', user_key, cmsgpack.pack({credit=new_credit}))
redis.call('SET', tx_key, cmsgpack.pack(rec))
redis.call('SADD', prep_user_key, txn_id)
return {0, cmsgpack.pack(rec)}
"""

# Lua script for atomic commit_transaction
# KEYS[1] = txn_key
PAYMENT_COMMIT_LUA = """
local tx_key = KEYS[1]

local raw = redis.call('GET', tx_key)
if not raw then
    return {-1, 'Unknown transaction'}
end
local rec = cmsgpack.unpack(raw)
local prep_user_key = 'txn:user:' .. rec['user_id'] .. ':prepared'
if rec['state'] == 'aborted' then
    return {-1, 'Transaction was already aborted'}
end
if rec['state'] == 'committed' then
    redis.call('SREM', prep_user_key, rec['txn_id'])
    return {0, raw}
end
if rec['state'] ~= 'prepared' then
    return {-1, 'Transaction in invalid state'}
end

rec['state'] = 'committed'
local packed = cmsgpack.pack(rec)
redis.call('SET', tx_key, packed)
redis.call('SREM', prep_user_key, rec['txn_id'])
return {0, packed}
"""

# Lua script for atomic abort_transaction
# KEYS[1] = txn_key
PAYMENT_ABORT_LUA = """
local tx_key = KEYS[1]

local raw = redis.call('GET', tx_key)
if not raw then
    return {0, 'aborted'}
end
local rec = cmsgpack.unpack(raw)
local user_key = rec['user_id']
local prep_user_key = 'txn:user:' .. rec['user_id'] .. ':prepared'
if rec['state'] == 'committed' then
    return {-1, 'Cannot abort committed transaction'}
end
if rec['state'] == 'aborted' then
    redis.call('SREM', prep_user_key, rec['txn_id'])
    return {0, raw}
end
if rec['state'] ~= 'prepared' then
    return {-1, 'Transaction in invalid state'}
end

local user_raw = redis.call('GET', user_key)
if user_raw then
    local user = cmsgpack.unpack(user_raw)
    user['credit'] = user['credit'] - tonumber(rec['delta'])
    redis.call('SET', user_key, cmsgpack.pack(user))
end

rec['state'] = 'aborted'
local packed = cmsgpack.pack(rec)
redis.call('SET', tx_key, packed)
redis.call('SREM', prep_user_key, rec['txn_id'])
return {0, packed}
"""


class PaymentRepository:
    def __init__(self, db: Redis):
        self.db = db
        self._prepare_script = db.register_script(PAYMENT_PREPARE_LUA)
        self._commit_script = db.register_script(PAYMENT_COMMIT_LUA)
        self._abort_script = db.register_script(PAYMENT_ABORT_LUA)

    @staticmethod
    def _decode_str(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    async def create_user(self, user_id: str, credit: int = 0):
        try:
            await self.db.set(user_id, msgpack.encode(UserValue(credit=credit)))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def batch_init_users(self, kv_pairs: dict[str, bytes]):
        try:
            await self.db.mset(kv_pairs)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_user(self, user_id: str) -> UserValue | None:
        try:
            raw = await self.db.get(user_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return self._decode_user(raw)

    async def get_user_or_error(self, user_id: str) -> UserValue:
        user = await self.get_user(user_id)
        if user is None:
            raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")
        return user

    async def save_user(self, user_id: str, user: UserValue):
        try:
            await self.db.set(user_id, msgpack.encode(user))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_prepare_record(self, txn_id: str) -> PrepareRecord | None:
        try:
            raw = await self.db.get(txn_key(txn_id))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return self._decode_prepare(raw)

    async def save_prepare_record(self, rec: PrepareRecord):
        try:
            await self.db.set(txn_key(rec.txn_id), msgpack.encode(rec))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def list_prepared_tx_ids(self) -> list[str]:
        prepared: list[str] = []
        try:
            async for raw_key in self.db.scan_iter(match="txn:*"):
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                if key.startswith("txn:user:"):
                    continue

                raw_record = await self.db.get(key)
                if raw_record is None:
                    continue

                record = self._decode_prepare(raw_record)
                if record.state == TxnState.PREPARED:
                    prepared.append(record.txn_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return prepared

    async def has_active_prepare(self, user_id: str) -> bool:
        try:
            return int(await self.db.scard(prepared_user_key(user_id))) > 0
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def prepare_transaction(self, txn_id: str, user_id: str, amount: int) -> PrepareRecord:
        tx_key = txn_key(txn_id)
        prep_key = prepared_user_key(user_id)
        try:
            result = await self._prepare_script(
                keys=[user_id, tx_key, prep_key],
                args=[txn_id, user_id, amount],
            )
            status_code = int(result[0])
            payload = result[1]
            if status_code == -2:
                raise HTTPException(status_code=400, detail=self._decode_str(payload))
            if status_code == -1:
                raise HTTPException(status_code=400, detail=self._decode_str(payload))
            if not isinstance(payload, (bytes, bytearray)):
                raise HTTPException(status_code=400, detail=DB_ERROR_STR)
            return self._decode_prepare(bytes(payload))
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def commit_transaction(self, txn_id: str) -> PrepareRecord:
        tx_key = txn_key(txn_id)
        try:
            result = await self._commit_script(keys=[tx_key], args=[txn_id])
            status_code = int(result[0])
            payload = result[1]
            if status_code == 0:
                if not isinstance(payload, (bytes, bytearray)):
                    raise HTTPException(status_code=400, detail=DB_ERROR_STR)
                return self._decode_prepare(bytes(payload))
            detail = self._decode_str(payload)
            raise HTTPException(status_code=400, detail=f"Transaction {txn_id}: {detail}")
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def abort_transaction(self, txn_id: str) -> bool:
        tx_key = txn_key(txn_id)
        try:
            result = await self._abort_script(keys=[tx_key], args=[])
            status_code = int(result[0])
            return status_code == 0
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def pay(self, user_id: str, amount: int) -> int:
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(user_id, prepared_user_key(user_id))
                        if int(await pipe.scard(prepared_user_key(user_id))) > 0:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"User: {user_id} has a prepared transaction in progress",
                            )

                        raw_user = await pipe.get(user_id)
                        if raw_user is None:
                            await pipe.unwatch()
                            raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")

                        user_entry = self._decode_user(raw_user)
                        user_entry.credit -= int(amount)
                        if user_entry.credit < 0:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"User: {user_id} credit cannot get reduced below zero!",
                            )

                        pipe.multi()
                        pipe.set(user_id, msgpack.encode(user_entry))
                        await pipe.execute()
                        return user_entry.credit
                    except redis.WatchError:
                        continue
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def saga_debit(self, tx_id: str, user_id: str, amount: int) -> tuple[bool, bool, str | None]:
        debit_key = f"saga:payment:debit:{tx_id}"
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(user_id, debit_key)

                        existing_raw = await pipe.get(debit_key)
                        if existing_raw is not None:
                            existing = self._decode_saga_debit(existing_raw)
                            await pipe.unwatch()
                            if existing.user_id != user_id or existing.amount != amount:
                                return False, False, f"Debit tx {tx_id} parameter mismatch"
                            if existing.state in (SagaDebitState.DEBITED, SagaDebitState.REFUNDED):
                                return True, False, None
                            return False, False, "Debit previously failed (insufficient credit)"

                        user_raw = await pipe.get(user_id)
                        if user_raw is None:
                            await pipe.unwatch()
                            return False, False, f"User: {user_id} not found!"

                        user = self._decode_user(user_raw)
                        if user.credit < amount:
                            rec = SagaDebitRecord(
                                tx_id=tx_id,
                                user_id=user_id,
                                amount=amount,
                                old_credit=user.credit,
                                new_credit=user.credit,
                                state=SagaDebitState.FAILED,
                            )
                            pipe.multi()
                            pipe.set(debit_key, msgpack.encode(rec))
                            await pipe.execute()
                            return False, False, f"Insufficient credit for user {user_id}"

                        new_credit = user.credit - amount
                        rec = SagaDebitRecord(
                            tx_id=tx_id,
                            user_id=user_id,
                            amount=amount,
                            old_credit=user.credit,
                            new_credit=new_credit,
                            state=SagaDebitState.DEBITED,
                        )
                        pipe.multi()
                        pipe.set(user_id, msgpack.encode(UserValue(credit=new_credit)))
                        pipe.set(debit_key, msgpack.encode(rec))
                        await pipe.execute()
                        return True, False, None
                    except redis.WatchError:
                        continue
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def saga_refund(self, tx_id: str) -> tuple[bool, bool, str | None]:
        debit_key = f"saga:payment:debit:{tx_id}"
        try:
            existing_raw = await self.db.get(debit_key)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

        if existing_raw is None:
            return True, False, None

        existing = self._decode_saga_debit(existing_raw)
        if existing.state in (SagaDebitState.REFUNDED, SagaDebitState.FAILED):
            return True, False, None

        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(existing.user_id, debit_key)

                        rec_raw = await pipe.get(debit_key)
                        if rec_raw is None:
                            await pipe.unwatch()
                            return True, False, None

                        rec = self._decode_saga_debit(rec_raw)
                        if rec.state in (SagaDebitState.REFUNDED, SagaDebitState.FAILED):
                            await pipe.unwatch()
                            return True, False, None

                        user_raw = await pipe.get(rec.user_id)
                        if user_raw is None:
                            await pipe.unwatch()
                            return False, True, f"User: {rec.user_id} not found during refund"

                        user = self._decode_user(user_raw)
                        updated_rec = SagaDebitRecord(
                            tx_id=rec.tx_id,
                            user_id=rec.user_id,
                            amount=rec.amount,
                            old_credit=rec.old_credit,
                            new_credit=rec.new_credit,
                            state=SagaDebitState.REFUNDED,
                        )
                        pipe.multi()
                        pipe.set(rec.user_id, msgpack.encode(UserValue(credit=user.credit + rec.amount)))
                        pipe.set(debit_key, msgpack.encode(updated_rec))
                        await pipe.execute()
                        return True, False, None
                    except redis.WatchError:
                        continue
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    @staticmethod
    def _decode_user(raw: bytes) -> UserValue:
        try:
            return msgpack.decode(raw, type=UserValue)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    @staticmethod
    def _decode_prepare(raw: bytes) -> PrepareRecord:
        try:
            return msgpack.decode(raw, type=PrepareRecord)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    @staticmethod
    def _decode_saga_debit(raw: bytes) -> SagaDebitRecord:
        try:
            return msgpack.decode(raw, type=SagaDebitRecord)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
