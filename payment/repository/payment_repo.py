import redis.asyncio as redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError

from models import DB_ERROR_STR, PrepareRecord, TxnState, UserValue


def txn_key(txn_id: str) -> str:
    return f"txn:{txn_id}"


def prepared_user_key(user_id: str) -> str:
    return f"txn:user:{user_id}:prepared"


class PaymentRepository:
    def __init__(self, db: Redis):
        self.db = db

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
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(user_id, tx_key)
                        existing_raw = await pipe.get(tx_key)
                        if existing_raw is not None:
                            existing = self._decode_prepare(existing_raw)
                            if existing.user_id != user_id or int(existing.delta) != -int(amount):
                                await pipe.unwatch()
                                raise HTTPException(status_code=400, detail=f"Transaction {txn_id} parameters mismatch")
                            if existing.state == TxnState.ABORTED:
                                await pipe.unwatch()
                                raise HTTPException(status_code=400, detail=f"Insufficient credit for user {user_id}")
                            await pipe.unwatch()
                            return existing

                        raw = await pipe.get(user_id)
                        if raw is None:
                            await pipe.unwatch()
                            raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")

                        current_user = self._decode_user(raw)
                        actual_new_credit = current_user.credit - int(amount)
                        if actual_new_credit < 0:
                            rec = PrepareRecord(
                                txn_id=txn_id,
                                user_id=user_id,
                                delta=-int(amount),
                                old_credit=current_user.credit,
                                new_credit=current_user.credit,
                                state=TxnState.ABORTED,
                            )
                            pipe.multi()
                            pipe.set(tx_key, msgpack.encode(rec))
                            await pipe.execute()
                            raise HTTPException(status_code=400, detail=f"Insufficient credit for user {user_id}")

                        rec = PrepareRecord(
                            txn_id=txn_id,
                            user_id=user_id,
                            delta=-int(amount),
                            old_credit=current_user.credit,
                            new_credit=actual_new_credit,
                            state=TxnState.PREPARED,
                        )
                        pipe.multi()
                        pipe.set(user_id, msgpack.encode(UserValue(credit=actual_new_credit)))
                        pipe.set(tx_key, msgpack.encode(rec))
                        pipe.sadd(prepared_user_key(user_id), txn_id)
                        await pipe.execute()
                        return rec
                    except redis.WatchError:
                        continue
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def commit_transaction(self, txn_id: str) -> PrepareRecord:
        tx_key = txn_key(txn_id)
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(tx_key)
                        raw = await pipe.get(tx_key)
                        if raw is None:
                            await pipe.unwatch()
                            raise HTTPException(status_code=400, detail=f"Unknown transaction {txn_id}")

                        rec = self._decode_prepare(raw)
                        if rec.state == TxnState.ABORTED:
                            await pipe.unwatch()
                            raise HTTPException(status_code=400, detail=f"Transaction {txn_id} was already aborted")
                        if rec.state == TxnState.COMMITTED:
                            pipe.multi()
                            pipe.srem(prepared_user_key(rec.user_id), rec.txn_id)
                            await pipe.execute()
                            return rec
                        if rec.state != TxnState.PREPARED:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Transaction {txn_id} in invalid state {rec.state.value}",
                            )

                        rec = PrepareRecord(
                            txn_id=rec.txn_id,
                            user_id=rec.user_id,
                            delta=rec.delta,
                            old_credit=rec.old_credit,
                            new_credit=rec.new_credit,
                            state=TxnState.COMMITTED,
                        )
                        pipe.multi()
                        pipe.set(tx_key, msgpack.encode(rec))
                        pipe.srem(prepared_user_key(rec.user_id), rec.txn_id)
                        await pipe.execute()
                        return rec
                    except redis.WatchError:
                        continue
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def abort_transaction(self, txn_id: str) -> bool:
        tx_key = txn_key(txn_id)
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        raw_rec = await self.db.get(tx_key)
                        if raw_rec is None:
                            return True

                        rec = self._decode_prepare(raw_rec)
                        await pipe.watch(rec.user_id, tx_key)
                        raw_rec = await pipe.get(tx_key)
                        if raw_rec is None:
                            await pipe.unwatch()
                            return True

                        current_rec = self._decode_prepare(raw_rec)
                        if current_rec.state == TxnState.COMMITTED:
                            await pipe.unwatch()
                            return False
                        if current_rec.state == TxnState.ABORTED:
                            pipe.multi()
                            pipe.srem(prepared_user_key(current_rec.user_id), current_rec.txn_id)
                            await pipe.execute()
                            return True
                        if current_rec.state != TxnState.PREPARED:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Transaction {txn_id} in invalid state {current_rec.state.value}",
                            )

                        raw_user = await pipe.get(current_rec.user_id)
                        pipe.multi()
                        if raw_user is not None:
                            user = self._decode_user(raw_user)
                            user.credit -= current_rec.delta
                            pipe.set(current_rec.user_id, msgpack.encode(user))
                        updated = PrepareRecord(
                            txn_id=current_rec.txn_id,
                            user_id=current_rec.user_id,
                            delta=current_rec.delta,
                            old_credit=current_rec.old_credit,
                            new_credit=current_rec.new_credit,
                            state=TxnState.ABORTED,
                        )
                        pipe.set(tx_key, msgpack.encode(updated))
                        pipe.srem(prepared_user_key(current_rec.user_id), current_rec.txn_id)
                        await pipe.execute()
                        return True
                    except redis.WatchError:
                        continue
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
