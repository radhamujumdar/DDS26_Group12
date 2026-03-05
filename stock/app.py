import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


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
    Persisted in Redis under key  txn:<tx_id>:<item_id>
    Represents a prepared-but-not-yet-committed stock hold.

    state values:
        "prepared"  – stock deducted from available, waiting for commit/abort
        "committed" – transaction finished, reservation record can be cleaned up
        "aborted"   – transaction rolled back, stock already restored
    """
    tx_id: str
    item_id: str
    amount: int
    state: str   # "prepared" | "committed" | "aborted"


# ─────────────────────────────────────────────
# Redis key helpers
# ─────────────────────────────────────────────

def reservation_key(tx_id: str, item_id: str) -> str:
    return f"txn:{tx_id}:{item_id}"


# ─────────────────────────────────────────────
# Recovery on restart
# ─────────────────────────────────────────────

def recover_prepared_transactions():
    """
    On service startup, scan for any reservations still in 'prepared' state.
    These are transactions whose coordinator crashed before sending commit/abort.

    Policy (safe default): abort all dangling prepared reservations so that
    stock is not held forever.
    """
    count = 0
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
                    logger.warning(
                        f"[RECOVERY] DANGLING_PREPARE tx={res.tx_id} "
                        f"item={res.item_id} amount={res.amount}"
                    )
                    _do_abort(res.tx_id, res.item_id, res)
                    logger.info(
                        f"[RECOVERY] ABORTED tx={res.tx_id} item={res.item_id}"
                    )
                    count += 1

            if cursor == 0:
                break

        logger.info(f"[RECOVERY] COMPLETE total_aborted={count}")

    except redis.exceptions.RedisError as e:
        logger.error(f"[RECOVERY] Redis error during recovery scan: {e}")


def _do_abort(tx_id: str, item_id: str, res: Reservation) -> bool:
    """
    Core abort logic shared by the /abort endpoint and the recovery path.
    Restores the reserved stock and marks the reservation as 'aborted'.
    Returns True on success, False on error.
    """
    rkey = reservation_key(tx_id, item_id)
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
        logger.error(f"[ABORT] Redis error for tx={tx_id} item={item_id}: {e}")
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
    logger.debug(f"Item: {key} created")
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
    logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
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

@app.post('/2pc/prepare/<tx_id>/<item_id>/<int:amount>')
def prepare(tx_id: str, item_id: str, amount: int):
    """
    PREPARE phase – called by the Order coordinator.

    Atomically:
      1. Check that sufficient stock exists.
      2. Deduct `amount` from the item's stock (held in reservation).
      3. Persist a Reservation record in state 'prepared'.

    Returns 200 on success (vote YES), 400 on failure (vote NO).

    Idempotent: if a reservation for this (tx_id, item_id) already exists
    in 'prepared' state we return 200 without double-deducting.
    """
    logger.info(f"[PREPARE] START tx={tx_id} item={item_id} amount={amount}")
    rkey = reservation_key(tx_id, item_id)

    # ── idempotency check ──
    try:
        existing_raw = db.get(rkey)
    except redis.exceptions.RedisError:
        logger.error(f"[PREPARE] DB_ERROR tx={tx_id} item={item_id}")
        return abort(400, DB_ERROR_STR)

    if existing_raw is not None:
        existing: Reservation = msgpack.decode(existing_raw, type=Reservation)
        if existing.state == "prepared":
            logger.info(f"[PREPARE] IDEMPOTENT_YES tx={tx_id} item={item_id}")
            return jsonify({"status": "prepared"}), 200
        if existing.state == "committed":
            logger.info(f"[PREPARE] ALREADY_COMMITTED tx={tx_id} item={item_id}")
            return jsonify({"status": "already committed"}), 200
        if existing.state == "aborted":
            logger.warning(f"[PREPARE] PREVIOUSLY_ABORTED tx={tx_id} item={item_id}")
            return abort(400, f"Txn {tx_id}: previously aborted for item {item_id}")

    # ── optimistic locking via WATCH/MULTI/EXEC ──
    try:
        with db.pipeline(transaction=True) as pipe:
            while True:
                try:
                    pipe.watch(item_id)

                    item_raw = pipe.get(item_id)
                    if item_raw is None:
                        pipe.unwatch()
                        logger.warning(f"[PREPARE] ITEM_NOT_FOUND tx={tx_id} item={item_id}")
                        return abort(400, f"Item: {item_id} not found!")

                    item: StockValue = msgpack.decode(item_raw, type=StockValue)

                    if item.stock < amount:
                        pipe.unwatch()
                        logger.warning(
                            f"[PREPARE] INSUFFICIENT_STOCK tx={tx_id} item={item_id} "
                            f"have={item.stock} need={amount}"
                        )
                        return abort(400, f"Item: {item_id} insufficient stock "
                                         f"(have {item.stock}, need {amount})")

                    # Deduct stock and persist reservation atomically
                    item.stock -= amount
                    reservation = Reservation(
                        tx_id=tx_id,
                        item_id=item_id,
                        amount=amount,
                        state="prepared"
                    )

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.set(rkey, msgpack.encode(reservation))
                    pipe.execute()

                    logger.info(f"[PREPARE] VOTE_YES tx={tx_id} item={item_id} amount={amount}")
                    return jsonify({"status": "prepared"}), 200

                except redis.WatchError:
                    continue  # retry optimistic lock

    except redis.exceptions.RedisError:
        logger.error(f"[PREPARE] DB_ERROR tx={tx_id} item={item_id}")
        return abort(400, DB_ERROR_STR)


@app.post('/2pc/commit/<tx_id>/<item_id>/<int:amount>')
def commit(tx_id: str, item_id: str, amount: int):
    """
    COMMIT phase – called by the Order coordinator after all participants voted YES.

    Marks the reservation as 'committed'.
    The stock was already deducted during prepare, so no stock change is needed.
    The `amount` parameter is accepted for API compatibility but not used here —
    the reservation already stores the amount from prepare time.

    Idempotent: safe to call multiple times.
    """
    logger.info(f"[COMMIT] START tx={tx_id} item={item_id}")
    rkey = reservation_key(tx_id, item_id)

    try:
        raw = db.get(rkey)
    except redis.exceptions.RedisError:
        logger.error(f"[COMMIT] DB_ERROR tx={tx_id} item={item_id}")
        return abort(400, DB_ERROR_STR)

    if raw is None:
        logger.warning(f"[COMMIT] NO_RESERVATION tx={tx_id} item={item_id}")
        return jsonify({"status": "committed"}), 200

    res: Reservation = msgpack.decode(raw, type=Reservation)

    if res.state == "committed":
        logger.info(f"[COMMIT] IDEMPOTENT tx={tx_id} item={item_id}")
        return jsonify({"status": "committed"}), 200

    if res.state == "aborted":
        logger.error(f"[COMMIT] ALREADY_ABORTED tx={tx_id} item={item_id}")
        return abort(400, f"Txn {tx_id}: cannot commit, already aborted for item {item_id}")

    # state == "prepared" — mark committed
    res.state = "committed"
    try:
        db.set(rkey, msgpack.encode(res))
    except redis.exceptions.RedisError:
        logger.error(f"[COMMIT] DB_ERROR tx={tx_id} item={item_id}")
        return abort(400, DB_ERROR_STR)

    logger.info(f"[COMMIT] SUCCESS tx={tx_id} item={item_id}")
    return jsonify({"status": "committed"}), 200


@app.post('/2pc/abort/<tx_id>/<item_id>/<int:amount>')
def abort_txn(tx_id: str, item_id: str, amount: int):
    """
    ABORT phase – called by the Order coordinator when any participant voted NO
    or when a timeout/failure requires rollback.

    Restores the reserved stock and marks the reservation as 'aborted'.
    The `amount` parameter is accepted for API compatibility but not used here —
    the reservation already stores the amount from prepare time.

    Idempotent: safe to call multiple times.
    """
    logger.info(f"[ABORT] START tx={tx_id} item={item_id}")
    rkey = reservation_key(tx_id, item_id)

    try:
        raw = db.get(rkey)
    except redis.exceptions.RedisError:
        logger.error(f"[ABORT] DB_ERROR tx={tx_id} item={item_id}")
        return abort(400, DB_ERROR_STR)

    if raw is None:
        logger.warning(f"[ABORT] NO_RESERVATION tx={tx_id} item={item_id}")
        return jsonify({"status": "aborted"}), 200

    res: Reservation = msgpack.decode(raw, type=Reservation)

    if res.state == "aborted":
        logger.info(f"[ABORT] IDEMPOTENT tx={tx_id} item={item_id}")
        return jsonify({"status": "aborted"}), 200

    if res.state == "committed":
        logger.error(f"[ABORT] ALREADY_COMMITTED tx={tx_id} item={item_id}")
        return abort(400, f"Txn {tx_id}: cannot abort, already committed for item {item_id}")

    # state == "prepared" — restore stock
    success = _do_abort(tx_id, item_id, res)
    if not success:
        return abort(400, DB_ERROR_STR)

    logger.info(f"[ABORT] SUCCESS tx={tx_id} item={item_id} stock_restored={res.amount}")
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
    recover_prepared_transactions()