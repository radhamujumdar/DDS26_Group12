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
    except RedisError:
        return None
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


async def _do_abort(rec: PrepareRecord):
    db = get_db()
    try:
        async with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(rec.user_id, txn_key(rec.txn_id), prepared_user_key(rec.user_id))

                    raw = await pipe.get(rec.user_id)
                    if raw:
                        user: UserValue = msgpack.decode(raw, type=UserValue)
                        # Always restore this transaction's reserved delta.
                        user.credit -= rec.delta
                        pipe.multi()
                        pipe.set(rec.user_id, msgpack.encode(user))
                        rec.state = TXN_ABORTED
                        pipe.set(txn_key(rec.txn_id), msgpack.encode(rec))
                        pipe.srem(prepared_user_key(rec.user_id), rec.txn_id)
                        await pipe.execute()
                        return
                    else:
                        pipe.multi()
                        rec.state = TXN_ABORTED
                        pipe.set(txn_key(rec.txn_id), msgpack.encode(rec))
                        pipe.srem(prepared_user_key(rec.user_id), rec.txn_id)
                        await pipe.execute()
                        return

                except redis.WatchError:
                    continue

    except RedisError as exc:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc


# ─────────────────────────────────────────────
# 2PC Participant endpoints
# ─────────────────────────────────────────────

@app.post('/2pc/prepare/{txn_id}/{user_id}/{amount}')
async def prepare(txn_id: str, user_id: str, amount: int):
    db = get_db()
    existing = await get_prepare_record(txn_id)
    if existing:
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
    rec = await get_prepare_record(txn_id)
    if rec is None:
        raise HTTPException(status_code=400, detail=f"Unknown transaction {txn_id}")

    if rec.state == TXN_ABORTED:
        raise HTTPException(status_code=400, detail=f"Transaction {txn_id} was already aborted")

    if rec.state == TXN_COMMITTED:
        try:
            await db.srem(prepared_user_key(rec.user_id), rec.txn_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return {"status": TXN_COMMITTED, "txn_id": txn_id}

    rec.state = TXN_COMMITTED
    try:
        async with db.pipeline(transaction=True) as pipe:
            pipe.multi()
            pipe.set(txn_key(txn_id), msgpack.encode(rec))
            pipe.srem(prepared_user_key(rec.user_id), rec.txn_id)
            await pipe.execute()
    except RedisError as exc:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    return {"status": TXN_COMMITTED, "txn_id": txn_id}


@app.post('/2pc/abort/{txn_id}')
async def abort_txn(txn_id: str):
    db = get_db()
    rec = await get_prepare_record(txn_id)
    if rec is None:
        return {"status": TXN_ABORTED, "txn_id": txn_id} # TODO: Should this return "Transaction not found"?

    if rec.state == TXN_COMMITTED:
        raise HTTPException(status_code=400, detail=f"Transaction {txn_id} already committed, cannot abort")

    # TODO: Is this check needed? We remove the transaction from the prepared set, but the _do_abort sets the state to TXN_ABORTED AND removes it from the set atomically.
    if rec.state == TXN_ABORTED:
        try:
            await db.srem(prepared_user_key(rec.user_id), rec.txn_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return {"status": TXN_ABORTED, "txn_id": txn_id}

    await _do_abort(rec)
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
    db = get_db()
    if await has_active_prepare(user_id):
        raise HTTPException(
            status_code=400,
            detail=f"User: {user_id} has a prepared transaction in progress",
        )

    user_entry: UserValue = await get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        raise HTTPException(status_code=400, detail=f"User: {user_id} credit cannot get reduced below zero!")
    try:
        await db.set(user_id, msgpack.encode(user_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"User: {user_id} credit updated to: {user_entry.credit}", status_code=200)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
