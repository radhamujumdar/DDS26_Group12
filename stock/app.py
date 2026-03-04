import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


# ─────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────

class StockValue(Struct):
    stock: int
    price: int


class Reservation(Struct):
    """
    Persisted in Redis under key  txn:<txn_id>:<item_id>
    Represents a prepared-but-not-yet-committed stock hold.

    state values:
        "prepared"  – stock deducted from available, waiting for commit/abort
        "committed" – transaction finished, reservation record can be cleaned up
        "aborted"   – transaction rolled back, stock already restored
    """
    txn_id: str
    item_id: str
    amount: int
    state: str   # "prepared" | "committed" | "aborted"


# ─────────────────────────────────────────────
# Redis key helpers
# ─────────────────────────────────────────────

def reservation_key(txn_id: str, item_id: str) -> str:
    return f"txn:{txn_id}:{item_id}"


# ─────────────────────────────────────────────
# Recovery on restart
# ─────────────────────────────────────────────

def recover_prepared_transactions():
    """
    On service startup, scan for any reservations still in 'prepared' state.
    These are transactions whose coordinator crashed before sending commit/abort.

    Policy (safe default): abort all dangling prepared reservations so that
    stock is not held forever.  If your coordinator implements a query endpoint
    (e.g. GET /orders/txn/<txn_id>/status) you can ask it whether to commit
    or abort instead.
    """
    try:
        cursor = 0
        while True:
            cursor, keys = db.scan(cursor, match="txn:*", count=100)
            for key in keys:
                raw = db.get(key)
                if raw is None:
                    continue
                try:
                    res: Reservation = msgpack.decode(raw, type=Reservation)
                except Exception:
                    continue
                if res.state == "prepared":
                    app.logger.warning(
                        f"[RECOVERY] Found dangling prepared reservation "
                        f"txn={res.txn_id} item={res.item_id} amount={res.amount}. "
                        f"Aborting (restoring stock)."
                    )
                    _do_abort(res.txn_id, res.item_id, res)

            if cursor == 0:
                break
    except redis.exceptions.RedisError as e:
        app.logger.error(f"[RECOVERY] Redis error during recovery scan: {e}")
        # In recovery
        logger.warning(f"[RECOVERY] DANGLING_PREPARE txn={res.txn_id} item={res.item_id} amount={res.amount}")
        logger.info(f"[RECOVERY] ABORTED txn={res.txn_id} item={res.item_id}")
        logger.info(f"[RECOVERY] COMPLETE total_aborted={res.amount}")

def _do_abort(txn_id: str, item_id: str, res: Reservation) -> bool:
    """
    Core abort logic shared by the /abort endpoint and the recovery path.
    Restores the reserved stock and marks the reservation as 'aborted'.
    Returns True on success, False on error.
    """
    rkey = reservation_key(txn_id, item_id)
    try:
        with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    pipe.watch(item_id, rkey)

                    item_raw = pipe.get(item_id)
                    if item_raw is None:
                        # Item was deleted; just mark aborted
                        pipe.multi()
                        res.state = "aborted"
                        pipe.set(rkey, msgpack.encode(res))
                        pipe.execute()
                        return True

                    item: StockValue = msgpack.decode(item_raw, type=StockValue)
                    item.stock += res.amount

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    res.state = "aborted"
                    pipe.set(rkey, msgpack.encode(res))
                    pipe.execute()
                    return True

                except redis.WatchError:
                    continue  # retry optimistic lock
    except redis.exceptions.RedisError as e:
        app.logger.error(f"[ABORT] Redis error for txn={txn_id} item={item_id}: {e}")
        return False


# ─────────────────────────────────────────────
# Existing CRUD endpoints (unchanged behaviour)
# ─────────────────────────────────────────────

def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify({"stock": item_entry.stock, "price": item_entry.price})


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


# ─────────────────────────────────────────────
# 2PC Participant endpoints
# ─────────────────────────────────────────────

@app.post('/stock/prepare/<txn_id>/<item_id>/<int:amount>')
def prepare(txn_id: str, item_id: str, amount: int):
    """
    PREPARE phase – called by the Order coordinator.

    Atomically:
      1. Check that sufficient stock exists.
      2. Deduct `amount` from the item's stock (held in reservation).
      3. Persist a Reservation record in state 'prepared'.

    Returns 200 on success (vote YES), 400 on failure (vote NO).

    Idempotent: if a reservation for this (txn_id, item_id) already exists
    in 'prepared' state we return 200 without double-deducting.
    """
    rkey = reservation_key(txn_id, item_id)

    # ── idempotency check ──
    try:
        existing_raw = db.get(rkey)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    if existing_raw is not None:
        existing: Reservation = msgpack.decode(existing_raw, type=Reservation)
        if existing.state == "prepared":
            return jsonify({"status": "prepared"}), 200
        if existing.state == "committed":
            return jsonify({"status": "already committed"}), 200
        if existing.state == "aborted":
            # Previously aborted – treat as a fresh failure (coordinator should not retry prepare after abort)
            return abort(400, f"Txn {txn_id}: previously aborted for item {item_id}")

    # ── optimistic locking via WATCH/MULTI/EXEC ──
    try:
        with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    pipe.watch(item_id)

                    item_raw = pipe.get(item_id)
                    if item_raw is None:
                        pipe.unwatch()
                        return abort(400, f"Item: {item_id} not found!")

                    item: StockValue = msgpack.decode(item_raw, type=StockValue)

                    if item.stock < amount:
                        pipe.unwatch()
                        return abort(400, f"Item: {item_id} insufficient stock "
                                         f"(have {item.stock}, need {amount})")

                    # Deduct stock and persist reservation atomically
                    item.stock -= amount
                    reservation = Reservation(
                        txn_id=txn_id,
                        item_id=item_id,
                        amount=amount,
                        state="prepared"
                    )

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.set(rkey, msgpack.encode(reservation))
                    pipe.execute()

                    app.logger.debug(
                        f"[PREPARE] txn={txn_id} item={item_id} amount={amount} – YES"
                    )
                    return jsonify({"status": "prepared"}), 200

                except redis.WatchError:
                    # Another write raced with us; retry
                    continue

    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # In prepare()
    logger.info(f"[PREPARE] START txn={txn_id} item={item_id} amount={amount}")
    logger.warning(f"[PREPARE] INSUFFICIENT STOCK txn={txn_id} item={item_id} have={item.stock} need={amount}")
    logger.info(f"[PREPARE] VOTE_YES txn={txn_id} item={item_id} amount={amount}")
    logger.error(f"[PREPARE] DB_ERROR txn={txn_id} item={item_id}")


@app.post('/stock/commit/<txn_id>/<item_id>')
def commit(txn_id: str, item_id: str):
    """
    COMMIT phase – called by the Order coordinator after all participants voted YES.

    Marks the reservation as 'committed'.
    The stock was already deducted during prepare, so no stock change is needed.

    Idempotent: safe to call multiple times.
    """
    rkey = reservation_key(txn_id, item_id)

    try:
        raw = db.get(rkey)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    if raw is None:
        # No reservation found – could mean prepare was never called or already cleaned up.
        # Treat as success so coordinator can move on.
        app.logger.warning(f"[COMMIT] No reservation found for txn={txn_id} item={item_id}")
        return jsonify({"status": "committed"}), 200

    res: Reservation = msgpack.decode(raw, type=Reservation)

    if res.state == "committed":
        return jsonify({"status": "committed"}), 200  # idempotent

    if res.state == "aborted":
        # Should never happen in a correct coordinator; log and error out
        app.logger.error(f"[COMMIT] txn={txn_id} item={item_id} – already aborted!")
        return abort(400, f"Txn {txn_id}: cannot commit, already aborted for item {item_id}")

    # state == "prepared" – mark committed
    res.state = "committed"
    try:
        db.set(rkey, msgpack.encode(res))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # In commit()
    logger.info(f"[COMMIT] START txn={txn_id} item={item_id}")
    logger.info(f"[COMMIT] SUCCESS txn={txn_id} item={item_id}")
    logger.warning(f"[COMMIT] NO_RESERVATION txn={txn_id} item={item_id}")
    logger.error(f"[COMMIT] ALREADY_ABORTED txn={txn_id} item={item_id}")

    app.logger.debug(f"[COMMIT] txn={txn_id} item={item_id} – committed")
    return jsonify({"status": "committed"}), 200
    


@app.post('/stock/abort/<txn_id>/<item_id>')
def abort_txn(txn_id: str, item_id: str):
    """
    ABORT phase – called by the Order coordinator when any participant voted NO
    or when a timeout/failure requires rollback.

    Restores the reserved stock and marks the reservation as 'aborted'.

    Idempotent: safe to call multiple times.
    """
    rkey = reservation_key(txn_id, item_id)

    try:
        raw = db.get(rkey)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    if raw is None:
        # prepare was never persisted (e.g. stock service crashed before writing reservation)
        # Nothing to restore – idempotent success.
        app.logger.warning(f"[ABORT] No reservation found for txn={txn_id} item={item_id} – nothing to undo")
        return jsonify({"status": "aborted"}), 200

    res: Reservation = msgpack.decode(raw, type=Reservation)

    if res.state == "aborted":
        return jsonify({"status": "aborted"}), 200  # idempotent

    if res.state == "committed":
        app.logger.error(f"[ABORT] txn={txn_id} item={item_id} – already committed, cannot abort!")
        return abort(400, f"Txn {txn_id}: cannot abort, already committed for item {item_id}")

    # state == "prepared" – restore stock
    success = _do_abort(txn_id, item_id, res)
    if not success:
        return abort(400, DB_ERROR_STR)
    
    # In abort()
    logger.info(f"[ABORT] START txn={txn_id} item={item_id}")
    logger.info(f"[ABORT] SUCCESS txn={txn_id} item={item_id} stock_restored={res.amount}")
    logger.error(f"[ABORT] ALREADY_COMMITTED txn={txn_id} item={item_id}")

    app.logger.debug(f"[ABORT] txn={txn_id} item={item_id} – aborted, stock restored")
    return jsonify({"status": "aborted"}), 200


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == '__main__':
    recover_prepared_transactions()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    # Run recovery after gunicorn forks the worker
    recover_prepared_transactions()

# ─────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
logger = logging.getLogger("stock-service")

