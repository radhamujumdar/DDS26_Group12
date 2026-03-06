import logging
import os
import uuid
import asyncio
from contextlib import suppress

import redis.asyncio as redis
from redis.exceptions import RedisError, WatchError
import httpx

from msgspec import msgpack, Struct
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from contextlib import asynccontextmanager


# ─────────────────────────────────────────────
# Logging setup  (must come before app + db)
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
logger = logging.getLogger("stock-service")


# ─────────────────────────────────────────────
# App + DB
# ─────────────────────────────────────────────

DB_ERROR_STR = "DB error"
DEFAULT_GATEWAY_URL = "http://gateway:80"
RECOVERY_INTERVAL_SECONDS = 2.0
RECOVERY_STARTUP_DELAY_SECONDS = 1.0


class StockValue(Struct):
    stock: int
    price: int


class Reservation(Struct):
    tx_id: str
    item_id: str
    amount: int
    state: str   # "prepared" | "committed" | "aborted"


# ─────────────────────────────────────────────
# Redis key helpers
# ─────────────────────────────────────────────

def reservation_key(tx_id: str, item_id: str) -> str:
    return f"txn:{tx_id}:{item_id}" # TODO: Do we need the item id in the identifier?


def _validate_positive_amount(amount: int, field_name: str = "amount"):
    if int(amount) <= 0:
        raise HTTPException(status_code=400, detail=f"{field_name} must be greater than zero")


def _get_gateway_url() -> str:
    return os.environ.get("GATEWAY_URL", DEFAULT_GATEWAY_URL).rstrip("/")


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
    the_app.state.gateway_url = _get_gateway_url()
    the_app.state.gateway_client = httpx.AsyncClient(timeout=2.0)
    the_app.state.recovery_task = asyncio.create_task(_recovery_loop())
    logger.info("[STARTUP] Stock service ready")
    yield
    the_app.state.recovery_task.cancel()
    with suppress(asyncio.CancelledError):
        await the_app.state.recovery_task
    await the_app.state.gateway_client.aclose()
    await the_app.state.db.aclose()
    logger.info("[SHUTDOWN] Stock service stopped")


app = FastAPI(title="stock-service", lifespan=lifespan)


def get_db() -> redis.Redis:
    return app.state.db


async def _get_coordinator_tx_state(tx_id: str) -> str | None:
    client: httpx.AsyncClient = app.state.gateway_client
    gateway_url: str = app.state.gateway_url
    try:
        response = await client.get(f"{gateway_url}/orders/2pc/tx/{tx_id}")
    except httpx.HTTPError:
        return None

    if response.status_code == 200:
        payload = response.json()
        return str(payload.get("state", ""))
    if response.status_code == 400:
        try:
            detail = str(response.json().get("detail", "")).lower()
        except ValueError:
            detail = ""
        if "not found" in detail:
            return "UNKNOWN"
    return None


async def _list_prepared_reservations() -> list[Reservation]:
    db = get_db()
    prepared: list[Reservation] = []
    async for raw_key in db.scan_iter(match="txn:*:*"):
        key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
        if key.count(":") != 2:
            continue

        try:
            raw_record = await db.get(key)
        except RedisError:
            continue
        if raw_record is None:
            continue

        try:
            reservation = msgpack.decode(raw_record, type=Reservation)
        except Exception:
            continue

        if reservation.state == "prepared":
            prepared.append(reservation)
    return prepared


def _decision_action(coordinator_state: str | None) -> str | None:
    if coordinator_state in {"COMMITTED", "COMMITTING"}:
        return "commit"
    if coordinator_state in {"ABORTED", "ABORTING", "UNKNOWN"}:
        return "abort"
    return None


async def recover_prepared_reservations_once() -> int:
    recovered = 0
    tx_state_cache: dict[str, str | None] = {}
    pending = await _list_prepared_reservations()
    for reservation in pending:
        tx_state = tx_state_cache.get(reservation.tx_id)
        if tx_state is None and reservation.tx_id not in tx_state_cache:
            tx_state = await _get_coordinator_tx_state(reservation.tx_id)
            tx_state_cache[reservation.tx_id] = tx_state

        action = _decision_action(tx_state)
        if action is None:
            continue

        try:
            if action == "commit":
                await commit(reservation.tx_id, reservation.item_id, reservation.amount)
            else:
                await abort_txn(reservation.tx_id, reservation.item_id, reservation.amount)
            recovered += 1
            logger.info(
                "[RECOVERY] finalized tx=%s item=%s action=%s coordinator_state=%s",
                reservation.tx_id,
                reservation.item_id,
                action,
                tx_state,
            )
        except HTTPException as exc:
            logger.warning(
                "[RECOVERY] failed tx=%s item=%s action=%s coordinator_state=%s detail=%s",
                reservation.tx_id,
                reservation.item_id,
                action,
                tx_state,
                exc.detail,
            )
    return recovered


async def _recovery_loop():
    await asyncio.sleep(RECOVERY_STARTUP_DELAY_SECONDS)
    while True:
        try:
            recovered = await recover_prepared_reservations_once()
            if recovered:
                logger.info("[RECOVERY] finalized_count=%s", recovered)
        except Exception as exc:
            logger.warning("[RECOVERY] loop_error=%s", exc)
        await asyncio.sleep(RECOVERY_INTERVAL_SECONDS)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

async def get_item_from_db(item_id: str) -> StockValue:
    db = get_db()
    try:
        entry: bytes = await db.get(item_id)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    if entry is None:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")
    return msgpack.decode(entry, type=StockValue)


async def _do_abort(tx_id: str, item_id: str, res: Reservation) -> bool:
    db = get_db()
    rkey = reservation_key(tx_id, item_id)
    try:
        async with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(item_id, rkey)

                    res_raw = await pipe.get(rkey)
                    if res_raw is None:
                        return True
                    current_res: Reservation = msgpack.decode(res_raw, type=Reservation)
                    if current_res.state == "aborted":
                        return True
                    if current_res.state == "committed":
                        return False

                    item_raw = await pipe.get(item_id)
                    if item_raw is None:
                        pipe.multi()
                        current_res.state = "aborted"
                        pipe.set(rkey, msgpack.encode(current_res))
                        await pipe.execute()
                        return True

                    item: StockValue = msgpack.decode(item_raw, type=StockValue)
                    item.stock += current_res.amount

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    current_res.state = "aborted"
                    pipe.set(rkey, msgpack.encode(current_res))
                    await pipe.execute()
                    return True

                except WatchError:
                    continue

    except RedisError as e:
        logger.error(f"[ABORT] Redis error for tx={tx_id} item={item_id}: {e}")
        return False


# ─────────────────────────────────────────────
# Existing CRUD endpoints
# ─────────────────────────────────────────────

@app.post('/item/create/{price}')
async def create_item(price: int):
    _validate_positive_amount(price, "price")
    db = get_db()
    key = str(uuid.uuid4())
    logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        await db.set(key, value)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"item_id": key}


@app.post('/batch_init/{n}/{starting_stock}/{item_price}')
async def batch_init_users(n: int, starting_stock: int, item_price: int):
    _validate_positive_amount(starting_stock, "starting_stock")
    _validate_positive_amount(item_price, "item_price")
    db = get_db()
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        await db.mset(kv_pairs)
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for stock successful"}


@app.get('/find/{item_id}')
async def find_item(item_id: str):
    item_entry: StockValue = await get_item_from_db(item_id)
    return {"stock": item_entry.stock, "price": item_entry.price}


# TODO: Do we need locking for the item amounts?
@app.post('/add/{item_id}/{amount}')
async def add_stock(item_id: str, amount: int):
    _validate_positive_amount(amount)
    db = get_db()
    item_entry: StockValue = await get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"Item: {item_id} stock updated to: {item_entry.stock}", status_code=200)

# TODO: Do we need locking for the item amounts?
@app.post('/subtract/{item_id}/{amount}')
async def remove_stock(item_id: str, amount: int):
    _validate_positive_amount(amount)
    db = get_db()
    item_entry: StockValue = await get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        await db.set(item_id, msgpack.encode(item_entry))
    except RedisError:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return PlainTextResponse(f"Item: {item_id} stock updated to: {item_entry.stock}", status_code=200)


# ─────────────────────────────────────────────
# 2PC Participant endpoints
# ─────────────────────────────────────────────

@app.post('/2pc/prepare/{tx_id}/{item_id}/{amount}')
async def prepare(tx_id: str, item_id: str, amount: int):
    _validate_positive_amount(amount)
    db = get_db()
    logger.info(f"[PREPARE] START tx={tx_id} item={item_id} amount={amount}")
    rkey = reservation_key(tx_id, item_id)

    # idempotency check
    try:
        existing_raw = await db.get(rkey)
    except RedisError:
        logger.error(f"[PREPARE] DB_ERROR tx={tx_id} item={item_id}")
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    if existing_raw is not None:
        existing: Reservation = msgpack.decode(existing_raw, type=Reservation)
        if int(existing.amount) != int(amount):
            logger.warning(
                f"[PREPARE] AMOUNT_MISMATCH tx={tx_id} item={item_id} expected={existing.amount} got={amount}"
            )
            raise HTTPException(
                status_code=400,
                detail=f"Txn {tx_id}: prepare amount mismatch for item {item_id}",
            )
        if existing.state == "prepared":
            logger.info(f"[PREPARE] IDEMPOTENT_YES tx={tx_id} item={item_id}")
            return {"status": "prepared"}
        if existing.state == "committed":
            logger.info(f"[PREPARE] ALREADY_COMMITTED tx={tx_id} item={item_id}")
            return {"status": "already committed"}
        if existing.state == "aborted":
            logger.warning(f"[PREPARE] PREVIOUSLY_ABORTED tx={tx_id} item={item_id}")
            raise HTTPException(status_code=400, detail=f"Txn {tx_id}: previously aborted for item {item_id}")

    # optimistic locking via WATCH/MULTI/EXEC
    try:
        async with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(item_id)

                    item_raw = await pipe.get(item_id)
                    if item_raw is None:
                        await pipe.unwatch()
                        logger.warning(f"[PREPARE] ITEM_NOT_FOUND tx={tx_id} item={item_id}")
                        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")

                    item: StockValue = msgpack.decode(item_raw, type=StockValue)

                    if item.stock < amount:
                        await pipe.unwatch()
                        logger.warning(f"[PREPARE] INSUFFICIENT_STOCK tx={tx_id} item={item_id} have={item.stock} need={amount}")
                        raise HTTPException(status_code=400, detail=f"Item: {item_id} insufficient stock (have {item.stock}, need {amount})")

                    item.stock -= amount
                    reservation = Reservation(tx_id=tx_id, item_id=item_id, amount=amount, state="prepared")

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.set(rkey, msgpack.encode(reservation))
                    await pipe.execute()

                    logger.info(f"[PREPARE] VOTE_YES tx={tx_id} item={item_id} amount={amount}")
                    return {"status": "prepared"}

                except WatchError:
                    continue

    except HTTPException:
        raise
    except RedisError:
        logger.error(f"[PREPARE] DB_ERROR tx={tx_id} item={item_id}")
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)


@app.post('/2pc/commit/{tx_id}/{item_id}/{amount}')
async def commit(tx_id: str, item_id: str, amount: int):
    _validate_positive_amount(amount)
    db = get_db()
    logger.info(f"[COMMIT] START tx={tx_id} item={item_id}")
    rkey = reservation_key(tx_id, item_id)

    try:
        async with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    await pipe.watch(rkey)
                    raw = await pipe.get(rkey)
                    if raw is None:
                        await pipe.unwatch()
                        logger.warning(f"[COMMIT] NO_RESERVATION tx={tx_id} item={item_id}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Unknown transaction {tx_id} for item {item_id}",
                        )

                    res: Reservation = msgpack.decode(raw, type=Reservation)
                    if int(amount) != int(res.amount):
                        await pipe.unwatch()
                        logger.error(
                            f"[COMMIT] AMOUNT_MISMATCH tx={tx_id} item={item_id} expected={res.amount} got={amount}"
                        )
                        raise HTTPException(
                            status_code=400,
                            detail=f"Txn {tx_id}: commit amount mismatch for item {item_id}",
                        )

                    if res.state == "committed":
                        await pipe.unwatch()
                        logger.info(f"[COMMIT] IDEMPOTENT tx={tx_id} item={item_id}")
                        return {"status": "committed"}

                    if res.state == "aborted":
                        await pipe.unwatch()
                        logger.error(f"[COMMIT] ALREADY_ABORTED tx={tx_id} item={item_id}")
                        raise HTTPException(
                            status_code=400,
                            detail=f"Txn {tx_id}: cannot commit, already aborted for item {item_id}",
                        )

                    if res.state != "prepared":
                        await pipe.unwatch()
                        raise HTTPException(
                            status_code=400,
                            detail=f"Txn {tx_id}: invalid state {res.state} for item {item_id}",
                        )

                    res.state = "committed"
                    pipe.multi()
                    pipe.set(rkey, msgpack.encode(res))
                    await pipe.execute()

                    logger.info(f"[COMMIT] SUCCESS tx={tx_id} item={item_id}")
                    return {"status": "committed"}
                except WatchError:
                    continue
    except HTTPException:
        raise
    except RedisError:
        logger.error(f"[COMMIT] DB_ERROR tx={tx_id} item={item_id}")
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)


@app.post('/2pc/abort/{tx_id}/{item_id}/{amount}')
async def abort_txn(tx_id: str, item_id: str, amount: int):
    _validate_positive_amount(amount)
    logger.info(f"[ABORT] START tx={tx_id} item={item_id}")
    rkey = reservation_key(tx_id, item_id)
    db = get_db()

    try:
        raw = await db.get(rkey)
    except RedisError:
        logger.error(f"[ABORT] DB_ERROR tx={tx_id} item={item_id}")
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    if raw is None:
        logger.warning(f"[ABORT] NO_RESERVATION tx={tx_id} item={item_id}")
        return {"status": "aborted"}

    res: Reservation = msgpack.decode(raw, type=Reservation)

    if int(amount) != int(res.amount):
        logger.error(
            f"[ABORT] AMOUNT_MISMATCH tx={tx_id} item={item_id} expected={res.amount} got={amount}"
        )
        raise HTTPException(
            status_code=400,
            detail=f"Txn {tx_id}: abort amount mismatch for item {item_id}",
        )

    if res.state == "aborted":
        logger.info(f"[ABORT] IDEMPOTENT tx={tx_id} item={item_id}")
        return {"status": "aborted"}

    if res.state == "committed":
        logger.error(f"[ABORT] ALREADY_COMMITTED tx={tx_id} item={item_id}")
        raise HTTPException(status_code=400, detail=f"Txn {tx_id}: cannot abort, already committed for item {item_id}")

    success = await _do_abort(tx_id, item_id, res)
    if not success:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    logger.info(f"[ABORT] SUCCESS tx={tx_id} item={item_id} stock_restored={res.amount}")
    return {"status": "aborted"}


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == '__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
