import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request

DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int



# Transaction states
TXN_PREPARED  = "prepared"
TXN_COMMITTED = "committed"
TXN_ABORTED   = "aborted"

class PrepareRecord(Struct):
    """
    Durable record written to Redis during the prepare phase.
    Survives crashes so the service can recover on restart.
    """
    txn_id:      str
    user_id:     str
    delta:       int   # negative = debit, positive = credit
    old_credit:  int   # snapshot before change (needed for abort/undo)
    new_credit:  int   # value to write on commit
    state:       str   # prepared | committed | aborted

def txn_key(txn_id: str) -> str:
    """Redis key for a prepare record."""
    return f"txn:{txn_id}"

def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry

def get_prepare_record(txn_id: str) -> PrepareRecord | None:
    try:
        raw = db.get(txn_key(txn_id))
    except redis.exceptions.RedisError:
        return None
    return msgpack.decode(raw, type=PrepareRecord) if raw else None


def save_prepare_record(rec: PrepareRecord):
    try:
        db.set(txn_key(rec.txn_id), msgpack.encode(rec))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

def recover_pending_transactions():
    """
    Called once at startup.  Any transaction left in state=prepared
    means we crashed after writing the prepare record but before
    hearing the coordinator's decision.  Safe default: abort them.
    """
    try:
        keys = db.keys("txn:*")
    except redis.exceptions.RedisError:
        app.logger.warning("Recovery scan failed: Redis unavailable at startup")
        return

    for k in keys:
        try:
            raw = db.get(k)
            if not raw:
                continue
            rec: PrepareRecord = msgpack.decode(raw, type=PrepareRecord)
        except Exception:
            continue

        if rec.state == TXN_PREPARED:
            app.logger.warning(
                f"Recovery: aborting orphaned txn {rec.txn_id} for user {rec.user_id}"
            )
            _do_abort(rec)


def _do_abort(rec: PrepareRecord):
    """Undo a prepared-but-not-committed transaction (in-place, no HTTP)."""
    try:
        raw = db.get(rec.user_id)
        if raw:
            user: UserValue = msgpack.decode(raw, type=UserValue)
            # Only roll back if the user's credit matches what we wrote at prepare-time.
            # If it's already old_credit the prepare write never happened (idempotent).
            if user.credit == rec.new_credit:
                user.credit = rec.old_credit
                db.set(rec.user_id, msgpack.encode(user))
    except redis.exceptions.RedisError:
        app.logger.error(f"Recovery abort failed for txn {rec.txn_id}")
        return

    rec.state = TXN_ABORTED
    save_prepare_record(rec)


@app.post('/2pc/prepare/<txn_id>/<user_id>/<amount>')
def prepare(txn_id: str, user_id: str, amount: int):
    """
    Phase-1.  Body JSON: { "user_id": "...", "amount": <int>, "action": "pay"|"add_funds" }

    Idempotency: if we already have a record for txn_id just return its
    current state without touching the DB again.
    """
    existing = get_prepare_record(txn_id)
    if existing:
        # Already processed – return the saved outcome so the coordinator
        # can re-drive phase-2 safely.
        if existing.state == TXN_ABORTED:
            return jsonify({"status": "aborted", "txn_id": txn_id}), 409
        # prepared or committed both count as "yes vote" for phase-2 purposes
        return jsonify({"status": existing.state, "txn_id": txn_id}), 200

    user_id   = user_id
    amount    = amount

    user = get_user_from_db(user_id)

    new_credit = user.credit + amount

    if new_credit < 0:
        # Vote NO – record the abort so retries are idempotent
        rec = PrepareRecord(
            txn_id=txn_id,
            user_id=user_id,
            delta=amount,
            old_credit=user.credit,
            new_credit=user.credit,   # unchanged
            state=TXN_ABORTED,
        )
        save_prepare_record(rec)
        abort(409, f"Insufficient credit for user {user_id}")

    # Tentatively apply the change and persist the prepare record atomically-ish.
    # We write the user record first, then the txn record.  If we crash between
    # the two writes, recovery will see no txn record (or a prepared record) and
    # can clean up.
    rec = PrepareRecord(
        txn_id=txn_id,
        user_id=user_id,
        delta=amount,
        old_credit=user.credit,
        new_credit=new_credit,
        state=TXN_PREPARED,
    )

    pipe = db.pipeline(transaction=True)
    try:
        pipe.set(user_id, msgpack.encode(UserValue(credit=new_credit)))
        pipe.set(txn_key(txn_id), msgpack.encode(rec))
        pipe.execute()
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)

    return jsonify({"status": TXN_PREPARED, "txn_id": txn_id}), 200


@app.post('/2pc/commit/<txn_id>')
def commit(txn_id: str):
    """
    Phase-2 commit.  The user record is already updated; just mark the txn done.
    Idempotent: committing an already-committed txn is fine.
    """
    rec = get_prepare_record(txn_id)
    if rec is None:
        abort(404, f"Unknown transaction {txn_id}")

    if rec.state == TXN_ABORTED:
        # Coordinator sent commit after we voted no
        abort(409, f"Transaction {txn_id} was already aborted")

    if rec.state == TXN_COMMITTED:
        return jsonify({"status": TXN_COMMITTED, "txn_id": txn_id}), 200

    # state == prepared => finalize
    rec.state = TXN_COMMITTED
    save_prepare_record(rec)

    return jsonify({"status": TXN_COMMITTED, "txn_id": txn_id}), 200


@app.post('/2pc/abort/<txn_id>')
def abort_txn(txn_id: str):
    """
    Phase-2 abort.  Roll back the tentative write made during prepare.
    Idempotent: aborting an already-aborted txn is fine.
    """
    rec = get_prepare_record(txn_id)
    if rec is None:
        # Never prepared: nothing to undo. Return success so coordinator moves on.
        return jsonify({"status": TXN_ABORTED, "txn_id": txn_id}), 200

    if rec.state == TXN_COMMITTED:
        abort(409, f"Transaction {txn_id} already committed – cannot abort")

    if rec.state == TXN_ABORTED:
        return jsonify({"status": TXN_ABORTED, "txn_id": txn_id}), 200

    # state == prepared => undo the tentative write
    _do_abort(rec)

    return jsonify({"status": TXN_ABORTED, "txn_id": txn_id}), 200


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


with app.app_context():
    recover_pending_transactions()

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
