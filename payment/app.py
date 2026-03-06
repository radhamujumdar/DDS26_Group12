import logging
import os
import uuid

import redis.asyncio as redis
from redis.exceptions import RedisError

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from contextlib import asynccontextmanager


DB_ERROR_STR = "DB error"

logger = logging.getLogger("payment-service")


# ─────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────

class UserValue(Struct):
    credit: int


class PrepareRecord(Struct):
    txn_id:      str
    user_id:     str
    delta:       int
    old_credit:  int
    new_credit:  int
    state:       str   # "prepared" | "committed" | "aborted"


TXN_PREPARED  = "prepared"
TXN_COMMITTED = "committed"
TXN_ABORTED   = "aborted"


def txn_key(txn_id: str) -> str:
    return f"txn:{txn_id}"


def prepared_user_key(user_id: str) -> str:
    return f"txn:user:{user_id}:prepared"


def _validate_positive_amount(amount: int, field_name: str = "amount"):
    if int(amount) <= 0:
        raise HTTPException(status_code=400, detail=f"{field_name} must be greater than zero")


# ─────────────────────────────────────────────
# App + DB lifecycle
# ─────────────────────────────────────────────

@asynccontextmanager
async def lifespan(the_app: FastAPI):
    the_app.state.db = redis.Redis(
        host=os.environ['REDIS_HOST'],
        port=int(os.environ['REDIS_PORT']),
        password=os.environ['REDIS_PASSWORD'],
        db=int(os.environ['REDIS_DB']),
    )
    logger.info("[STARTUP] Payment service ready")
    yield
    await the_app.state.db.aclose()
    logger.info("[SHUTDOWN] Payment service stopped")


app = FastAPI(title="payment-service", lifespan=lifespan)


def get_db() -> redis.Redis:
    return app.state.db


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

async def get_user_from_db(user_id: str) -> UserValue:
    db = get_db()
    try:
        entry: bytes = await db.get(user_id)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    if entry is None:
        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")
    return msgpack.decode(entry, type=UserValue)


async def get_prepare_record(txn_id: str) -> PrepareRecord | None:
    db = get_db()
    try:
        raw = await db.get(txn_key(txn_id))
    except RedisError as exc:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
    return msgpack.decode(raw, type=PrepareRecord) if raw else None


async def save_prepare_record(rec: PrepareRecord):
    db = get_db()
    try:
        await db.set(txn_key(rec.txn_id), msgpack.encode(rec))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)


async def has_active_prepare(user_id: str) -> bool:
    db = get_db()
    try:
        return int(await db.scard(prepared_user_key(user_id))) > 0
    except RedisError as exc:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc


async def _do_abort(txn_id: str) -> bool:
    db = get_db()
    try:
        async with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    tx_key = txn_key(txn_id)
                    raw_rec = await db.get(tx_key)
                    if raw_rec is None:
                        return True

                    rec: PrepareRecord = msgpack.decode(raw_rec, type=PrepareRecord)
                    await pipe.watch(rec.user_id, tx_key)
                    raw_rec = await pipe.get(tx_key)
                    if raw_rec is None:
                        await pipe.unwatch()
                        return True

                    current_rec: PrepareRecord = msgpack.decode(raw_rec, type=PrepareRecord)
                    if current_rec.state == TXN_COMMITTED:
                        await pipe.unwatch()
                        return False
                    if current_rec.state == TXN_ABORTED:
                        pipe.multi()
                        pipe.srem(prepared_user_key(current_rec.user_id), current_rec.txn_id)
                        await pipe.execute()
                        return True

                    if current_rec.state != TXN_PREPARED:
                        await pipe.unwatch()
                        raise HTTPException(
                            status_code=400,
                            detail=f"Transaction {txn_id} in invalid state {current_rec.state}",
                        )

                    raw_user = await pipe.get(current_rec.user_id)
                    pipe.multi()
                    if raw_user is not None:
                        user: UserValue = msgpack.decode(raw_user, type=UserValue)
                        user.credit -= current_rec.delta
                        pipe.set(current_rec.user_id, msgpack.encode(user))
                    current_rec.state = TXN_ABORTED
                    pipe.set(tx_key, msgpack.encode(current_rec))
                    pipe.srem(prepared_user_key(current_rec.user_id), current_rec.txn_id)
                    await pipe.execute()
                    return True
                except redis.WatchError:
                    continue
    except HTTPException:
        raise
    except RedisError as exc:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc


# ─────────────────────────────────────────────
# 2PC Participant endpoints
# ─────────────────────────────────────────────

@app.post('/2pc/prepare/{txn_id}/{user_id}/{amount}')
async def prepare(txn_id: str, user_id: str, amount: int):
    db = get_db()
    _validate_positive_amount(amount)
    existing = await get_prepare_record(txn_id)
    if existing:
        if existing.user_id != user_id or int(existing.delta) != -int(amount):
            raise HTTPException(status_code=400, detail=f"Transaction {txn_id} parameters mismatch")
        if existing.state == TXN_ABORTED:
            raise HTTPException(status_code=400, detail=f"Insufficient credit for user {user_id}") # TODO: Update error message - a transaction can be aborted by orchestrator due to insufficient stock
        return {"status": existing.state, "txn_id": txn_id}

    user = await get_user_from_db(user_id)

    new_credit = user.credit - int(amount)

    if new_credit < 0:
        rec = PrepareRecord(
            txn_id=txn_id, user_id=user_id, delta=-int(amount),
            old_credit=user.credit, new_credit=user.credit,
            state=TXN_ABORTED,
        )
        await save_prepare_record(rec)
        raise HTTPException(status_code=400, detail=f"Insufficient credit for user {user_id}")

    rec = PrepareRecord(
        txn_id=txn_id, user_id=user_id, delta=-int(amount),
        old_credit=user.credit, new_credit=new_credit,
        state=TXN_PREPARED,
    )

    try:
        async with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(user_id)

                    # Re-read inside watch to avoid races
                    raw = await pipe.get(user_id)
                    if raw is None:
                        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")
                    current_user = msgpack.decode(raw, type=UserValue)

                    actual_new_credit = current_user.credit - int(amount)
                    if actual_new_credit < 0:
                        await pipe.unwatch()
                        rec.state = TXN_ABORTED
                        await save_prepare_record(rec)
                        raise HTTPException(status_code=400, detail=f"Insufficient credit for user {user_id}")

                    rec.old_credit = current_user.credit
                    rec.new_credit = actual_new_credit

                    pipe.multi()
                    pipe.set(user_id, msgpack.encode(UserValue(credit=actual_new_credit)))
                    pipe.set(txn_key(txn_id), msgpack.encode(rec))
                    pipe.sadd(prepared_user_key(user_id), txn_id)
                    await pipe.execute()
                    break

                except redis.WatchError:
                    continue

    except HTTPException:
        raise
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    return {"status": TXN_PREPARED, "txn_id": txn_id}


@app.post('/2pc/commit/{txn_id}')
async def commit(txn_id: str):
    db = get_db()
    tx_key = txn_key(txn_id)
    try:
        async with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(tx_key)
                    raw = await pipe.get(tx_key)
                    if raw is None:
                        await pipe.unwatch()
                        raise HTTPException(status_code=400, detail=f"Unknown transaction {txn_id}")

                    rec: PrepareRecord = msgpack.decode(raw, type=PrepareRecord)
                    if rec.state == TXN_ABORTED:
                        await pipe.unwatch()
                        raise HTTPException(status_code=400, detail=f"Transaction {txn_id} was already aborted")
                    if rec.state == TXN_COMMITTED:
                        pipe.multi()
                        pipe.srem(prepared_user_key(rec.user_id), rec.txn_id)
                        await pipe.execute()
                        return {"status": TXN_COMMITTED, "txn_id": txn_id}
                    if rec.state != TXN_PREPARED:
                        await pipe.unwatch()
                        raise HTTPException(
                            status_code=400,
                            detail=f"Transaction {txn_id} in invalid state {rec.state}",
                        )

                    rec.state = TXN_COMMITTED
                    pipe.multi()
                    pipe.set(tx_key, msgpack.encode(rec))
                    pipe.srem(prepared_user_key(rec.user_id), rec.txn_id)
                    await pipe.execute()
                    return {"status": TXN_COMMITTED, "txn_id": txn_id}
                except redis.WatchError:
                    continue
    except HTTPException:
        raise
    except RedisError as exc:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc


@app.post('/2pc/abort/{txn_id}')
async def abort_txn(txn_id: str):
    aborted = await _do_abort(txn_id)
    if not aborted:
        raise HTTPException(status_code=400, detail=f"Transaction {txn_id} already committed, cannot abort")
    return {"status": TXN_ABORTED, "txn_id": txn_id}


# ─────────────────────────────────────────────
# Original CRUD endpoints
# ─────────────────────────────────────────────

@app.post('/create_user')
async def create_user():
    db = get_db()
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db.set(key, value)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"user_id": key}


@app.post('/batch_init/{n}/{starting_money}')
async def batch_init_users(n: int, starting_money: int):
    _validate_positive_amount(starting_money, "starting_money")
    db = get_db()
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(UserValue(credit=starting_money))
        for i in range(n)
    }
    try:
        await db.mset(kv_pairs)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for users successful"}


@app.get('/find_user/{user_id}')
async def find_user(user_id: str):
    user_entry: UserValue = await get_user_from_db(user_id)
    return {"user_id": user_id, "credit": user_entry.credit}


@app.post('/add_funds/{user_id}/{amount}')
async def add_credit(user_id: str, amount: int):
    _validate_positive_amount(amount)
    db = get_db()
    user_entry: UserValue = await get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"User: {user_id} credit updated to: {user_entry.credit}", status_code=200)


@app.post('/pay/{user_id}/{amount}')
async def remove_credit(user_id: str, amount: int):
    _validate_positive_amount(amount)
    db = get_db()
    try:
        async with db.pipeline(transaction=True) as pipe:
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

                    user_entry: UserValue = msgpack.decode(raw_user, type=UserValue)
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
                    return PlainTextResponse(
                        f"User: {user_id} credit updated to: {user_entry.credit}",
                        status_code=200,
                    )
                except redis.WatchError:
                    continue
    except HTTPException:
        raise
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
