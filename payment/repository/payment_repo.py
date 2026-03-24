import asyncio
import uuid

import redis.asyncio as redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError

from models import DB_ERROR_STR, PrepareRecord, SagaDebitRecord, SagaDebitState, TxnState, UserValue

# ---------------------------------------------------------------------------
# Key helpers — all co-located on the same hash slot as the user_id UUID.
#
# Redis Cluster routes by hash tag: the first {…} substring in a key.
# A raw UUID key "abc-123" hashes on the full string "abc-123".
# A key "{abc-123}:anything" also hashes on "abc-123" (the hash-tag content).
# Therefore all keys below land on the same cluster slot as the user record.
# ---------------------------------------------------------------------------


def _user_key(user_id: str) -> str:
    """Primary user record key.  Prefixed to avoid collision with other services."""
    return f"pay:{{{user_id}}}"


def _user_txn_key(user_id: str, txn_id: str) -> str:
    """Transaction prepare record.  Slot = slot(user_id)."""
    return f"{{{user_id}}}:pay:txn:{txn_id}"


def _prepared_user_key(user_id: str) -> str:
    """Set of in-progress txn IDs for a user.  Slot = slot(user_id)."""
    return f"{{{user_id}}}:prepared"


def _user_debit_key(user_id: str, tx_id: str) -> str:
    """Saga debit record.  Slot = slot(user_id)."""
    return f"{{{user_id}}}:debit:{tx_id}"


def _txn_uid_index_key(txn_id: str) -> str:
    """Secondary index: txn_id → user_id (for commit/abort without user_id)."""
    return f"txn:uid:{txn_id}"


def _debit_uid_index_key(tx_id: str) -> str:
    """Secondary index: tx_id → user_id (for saga_refund without user_id)."""
    return f"debit:uid:{tx_id}"


def _user_lock_key(user_id: str) -> str:
    """Mutex lock for atomic read-modify-write on user data.  Slot = slot(user_id)."""
    return f"{{{user_id}}}:lock"


_RELEASE_USER_LOCK_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
"""


# ---------------------------------------------------------------------------
# Lua scripts — all KEYS are on the same hash slot (user_id).
# ---------------------------------------------------------------------------

# KEYS[1] = pay:{user_id} (prefixed user key)
# KEYS[2] = {user_id}:pay:txn:{txn_id}
# KEYS[3] = {user_id}:prepared
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

# KEYS[1] = {user_id}:txn:{txn_id}
PAYMENT_COMMIT_LUA = """
local tx_key = KEYS[1]

local raw = redis.call('GET', tx_key)
if not raw then
    return {-1, 'Unknown transaction'}
end
local rec = cmsgpack.unpack(raw)
local prep_user_key = '{' .. rec['user_id'] .. '}:prepared'
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

# KEYS[1] = {user_id}:txn:{txn_id}
PAYMENT_ABORT_LUA = """
local tx_key = KEYS[1]

local raw = redis.call('GET', tx_key)
if not raw then
    return {0, 'aborted'}
end
local rec = cmsgpack.unpack(raw)
local user_key = 'pay:{' .. rec['user_id'] .. '}'
local prep_user_key = '{' .. rec['user_id'] .. '}:prepared'
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

    async def _acquire_user_lock(self, user_id: str, ttl_ms: int = 5000) -> str | None:
        token = str(uuid.uuid4())
        acquired = await self.db.set(_user_lock_key(user_id), token, nx=True, px=ttl_ms)
        return token if acquired else None

    async def _release_user_lock(self, user_id: str, token: str) -> None:
        try:
            await self.db.eval(_RELEASE_USER_LOCK_SCRIPT, 1, _user_lock_key(user_id), token)
        except RedisError:
            pass

    @staticmethod
    def _decode_str(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    async def create_user(self, user_id: str, credit: int = 0):
        try:
            await self.db.set(_user_key(user_id), msgpack.encode(UserValue(credit=credit)))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def batch_init_users(self, kv_pairs: dict[str, bytes]):
        try:
            async with self.db.pipeline(transaction=False) as pipe:
                for k, v in kv_pairs.items():
                    pipe.set(_user_key(k), v)
                await pipe.execute()
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_user(self, user_id: str) -> UserValue | None:
        try:
            raw = await self.db.get(_user_key(user_id))
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
            await self.db.set(_user_key(user_id), msgpack.encode(user))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def _get_user_id_for_txn(self, txn_id: str) -> str | None:
        """Look up user_id via secondary index (needed for commit/abort by txn_id)."""
        try:
            raw = await self.db.get(_txn_uid_index_key(txn_id))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else str(raw)

    async def get_prepare_record(self, txn_id: str) -> PrepareRecord | None:
        user_id = await self._get_user_id_for_txn(txn_id)
        if user_id is None:
            return None
        try:
            raw = await self.db.get(_user_txn_key(user_id, txn_id))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return self._decode_prepare(raw)

    async def save_prepare_record(self, rec: PrepareRecord):
        try:
            await self.db.set(_user_txn_key(rec.user_id, rec.txn_id), msgpack.encode(rec))
            await self.db.set(_txn_uid_index_key(rec.txn_id), rec.user_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def list_prepared_tx_ids(self) -> list[str]:
        prepared: list[str] = []
        try:
            async for raw_key in self.db.scan_iter(match="*:pay:txn:*"):
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                if not key.startswith("{"):
                    continue
                raw_record = await self.db.get(key)
                if raw_record is None:
                    continue
                try:
                    record = self._decode_prepare(raw_record)
                except HTTPException:
                    continue
                if record.state == TxnState.PREPARED:
                    prepared.append(record.txn_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return prepared

    async def has_active_prepare(self, user_id: str) -> bool:
        try:
            return int(await self.db.scard(_prepared_user_key(user_id))) > 0
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def prepare_transaction(self, txn_id: str, user_id: str, amount: int) -> PrepareRecord:
        tx_key = _user_txn_key(user_id, txn_id)
        prep_key = _prepared_user_key(user_id)
        # Secondary index set before Lua (best-effort; commit/abort depend on it).
        try:
            await self.db.set(_txn_uid_index_key(txn_id), user_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        try:
            result = await self._prepare_script(
                keys=[_user_key(user_id), tx_key, prep_key],
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
        user_id = await self._get_user_id_for_txn(txn_id)
        if user_id is None:
            raise HTTPException(status_code=400, detail=f"Transaction {txn_id}: unknown txn_id")
        tx_key = _user_txn_key(user_id, txn_id)
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
        user_id = await self._get_user_id_for_txn(txn_id)
        if user_id is None:
            return True  # nothing was prepared, treat as already aborted
        tx_key = _user_txn_key(user_id, txn_id)
        try:
            result = await self._abort_script(keys=[tx_key], args=[])
            status_code = int(result[0])
            return status_code == 0
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def pay(self, user_id: str, amount: int) -> int:
        for _ in range(50):
            token = await self._acquire_user_lock(user_id)
            if token is None:
                await asyncio.sleep(0.05)
                continue
            try:
                if int(await self.db.scard(_prepared_user_key(user_id))) > 0:
                    raise HTTPException(
                        status_code=400,
                        detail=f"User: {user_id} has a prepared transaction in progress",
                    )
                raw_user = await self.db.get(_user_key(user_id))
                if raw_user is None:
                    raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")
                user_entry = self._decode_user(raw_user)
                user_entry.credit -= int(amount)
                if user_entry.credit < 0:
                    raise HTTPException(
                        status_code=400,
                        detail=f"User: {user_id} credit cannot get reduced below zero!",
                    )
                await self.db.set(_user_key(user_id), msgpack.encode(user_entry))
                return user_entry.credit
            except HTTPException:
                raise
            except RedisError as exc:
                raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
            finally:
                await self._release_user_lock(user_id, token)
        raise HTTPException(status_code=400, detail=f"Could not acquire lock for user {user_id}")

    async def saga_debit(self, tx_id: str, user_id: str, amount: int) -> tuple[bool, bool, str | None]:
        debit_key = _user_debit_key(user_id, tx_id)
        # Secondary index for saga_refund (may arrive without user_id).
        try:
            await self.db.set(_debit_uid_index_key(tx_id), user_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        for _ in range(50):
            token = await self._acquire_user_lock(user_id)
            if token is None:
                await asyncio.sleep(0.05)
                continue
            try:
                existing_raw = await self.db.get(debit_key)
                if existing_raw is not None:
                    existing = self._decode_saga_debit(existing_raw)
                    if existing.user_id != user_id or existing.amount != amount:
                        return False, False, f"Debit tx {tx_id} parameter mismatch"
                    if existing.state in (SagaDebitState.DEBITED, SagaDebitState.REFUNDED):
                        return True, False, None
                    return False, False, "Debit previously failed (insufficient credit)"

                user_raw = await self.db.get(_user_key(user_id))
                if user_raw is None:
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
                    await self.db.set(debit_key, msgpack.encode(rec))
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
                await self.db.set(_user_key(user_id), msgpack.encode(UserValue(credit=new_credit)))
                await self.db.set(debit_key, msgpack.encode(rec))
                return True, False, None
            except HTTPException:
                raise
            except RedisError as exc:
                raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
            finally:
                await self._release_user_lock(user_id, token)
        return False, True, f"Could not acquire lock for user {user_id}"

    async def saga_refund(
        self,
        tx_id: str,
        user_id: str | None = None,
        amount: int | None = None,
    ) -> tuple[bool, bool, str | None]:
        # Resolve user_id if not supplied (HTTP refund endpoint omits it).
        if user_id is None:
            try:
                raw_uid = await self.db.get(_debit_uid_index_key(tx_id))
            except RedisError as exc:
                raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
            if raw_uid is None:
                return True, False, None  # no debit was recorded → nothing to refund
            user_id = raw_uid.decode() if isinstance(raw_uid, bytes) else str(raw_uid)

        debit_key = _user_debit_key(user_id, tx_id)
        try:
            existing_raw = await self.db.get(debit_key)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

        if existing_raw is None:
            if amount is None:
                return True, False, None

            refunded = SagaDebitRecord(
                tx_id=tx_id,
                user_id=user_id,
                amount=amount,
                old_credit=0,
                new_credit=0,
                state=SagaDebitState.REFUNDED,
            )
            try:
                created = await self.db.set(debit_key, msgpack.encode(refunded), nx=True)
            except RedisError as exc:
                raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
            if created:
                return True, False, None
            try:
                existing_raw = await self.db.get(debit_key)
            except RedisError as exc:
                raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
            if existing_raw is None:
                return True, False, None

        existing = self._decode_saga_debit(existing_raw)
        if user_id is not None and existing.user_id != user_id:
            return False, False, f"Refund tx {tx_id} parameter mismatch"
        if amount is not None and existing.amount != amount:
            return False, False, f"Refund tx {tx_id} parameter mismatch"
        if existing.state in (SagaDebitState.REFUNDED, SagaDebitState.FAILED):
            return True, False, None

        for _ in range(50):
            token = await self._acquire_user_lock(existing.user_id)
            if token is None:
                await asyncio.sleep(0.05)
                continue
            try:
                rec_raw = await self.db.get(debit_key)
                if rec_raw is None:
                    return True, False, None
                rec = self._decode_saga_debit(rec_raw)
                if rec.state in (SagaDebitState.REFUNDED, SagaDebitState.FAILED):
                    return True, False, None
                user_raw = await self.db.get(_user_key(rec.user_id))
                if user_raw is None:
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
                await self.db.set(_user_key(rec.user_id), msgpack.encode(UserValue(credit=user.credit + rec.amount)))
                await self.db.set(debit_key, msgpack.encode(updated_rec))
                return True, False, None
            except HTTPException:
                raise
            except RedisError as exc:
                raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
            finally:
                await self._release_user_lock(existing.user_id, token)
        return False, True, f"Could not acquire lock for user {existing.user_id}"

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
