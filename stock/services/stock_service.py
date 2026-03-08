import logging
import time
import uuid

from fastapi import HTTPException
from msgspec import msgpack

from logging_utils import log_event
from models import ReservationState, StockValue
from repository.stock_repo import StockRepository
from redis.exceptions import RedisError, WatchError

class StockService:
    SAGA_KEY_TTL_SECONDS = 24 * 60 * 60
    def __init__(self, repo: StockRepository, logger: logging.Logger):
        self.repo = repo
        self.logger = logger

    async def create_item(self, price: int) -> dict:
        self._require_positive(price, "price")
        item_id = str(uuid.uuid4())
        await self.repo.create_item(item_id=item_id, price=price)
        self._log("item_created", item_id=item_id, price=price)
        return {"item_id": item_id}

    async def batch_init(self, n: int, starting_stock: int, item_price: int) -> dict:
        self._require_positive(n, "n")
        self._require_positive(starting_stock, "starting_stock")
        self._require_positive(item_price, "item_price")
        kv_pairs: dict[str, bytes] = {
            f"{idx}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
            for idx in range(n)
        }
        await self.repo.batch_init_items(kv_pairs)
        self._log("batch_init_items", count=n, starting_stock=starting_stock, item_price=item_price)
        return {"msg": "Batch init for stock successful"}

    async def find_item(self, item_id: str) -> dict:
        item = await self.repo.get_item_or_error(item_id)
        return {"stock": item.stock, "price": item.price}

    async def add_stock(self, item_id: str, amount: int) -> int:
        self._require_positive(amount, "amount")
        updated_stock = await self.repo.add_stock_non_atomic(item_id=item_id, amount=amount)
        self._log("stock_added", item_id=item_id, amount=amount, stock=updated_stock)
        return updated_stock

    async def remove_stock(self, item_id: str, amount: int) -> int:
        self._require_positive(amount, "amount")
        updated_stock = await self.repo.subtract_stock_non_atomic(item_id=item_id, amount=amount)
        self._log("stock_subtracted", item_id=item_id, amount=amount, stock=updated_stock)
        return updated_stock

    async def prepare(self, tx_id: str, item_id: str, amount: int) -> dict:
        self._require_positive(amount, "amount")
        started = time.perf_counter()
        self._log("prepare_begin", tx_id=tx_id, item_id=item_id, amount=amount)
        try:
            status = await self.repo.prepare_reservation(tx_id=tx_id, item_id=item_id, amount=amount)
            self._log(
                "prepare_complete",
                tx_id=tx_id,
                item_id=item_id,
                amount=amount,
                status=status,
                duration_ms=self._duration_ms(started),
            )
            return {"status": status}
        except HTTPException as exc:
            self._log(
                "prepare_failed",
                level="warning",
                tx_id=tx_id,
                item_id=item_id,
                amount=amount,
                detail=str(exc.detail),
                duration_ms=self._duration_ms(started),
            )
            raise

    async def commit(self, tx_id: str, item_id: str, amount: int) -> dict:
        self._require_positive(amount, "amount")
        started = time.perf_counter()
        self._log("commit_begin", tx_id=tx_id, item_id=item_id, amount=amount)
        try:
            status = await self.repo.commit_reservation(tx_id=tx_id, item_id=item_id, amount=amount)
            self._log(
                "commit_complete",
                tx_id=tx_id,
                item_id=item_id,
                amount=amount,
                status=status,
                duration_ms=self._duration_ms(started),
            )
            return {"status": status}
        except HTTPException as exc:
            self._log(
                "commit_failed",
                level="warning",
                tx_id=tx_id,
                item_id=item_id,
                amount=amount,
                detail=str(exc.detail),
                duration_ms=self._duration_ms(started),
            )
            raise

    async def abort(self, tx_id: str, item_id: str, amount: int) -> dict:
        self._require_positive(amount, "amount")
        started = time.perf_counter()
        self._log("abort_begin", tx_id=tx_id, item_id=item_id, amount=amount)
        try:
            status = await self.repo.abort_reservation(tx_id=tx_id, item_id=item_id, amount=amount)
            self._log(
                "abort_complete",
                tx_id=tx_id,
                item_id=item_id,
                amount=amount,
                status=status,
                duration_ms=self._duration_ms(started),
            )
            return {"status": status}
        except HTTPException as exc:
            self._log(
                "abort_failed",
                level="warning",
                tx_id=tx_id,
                item_id=item_id,
                amount=amount,
                detail=str(exc.detail),
                duration_ms=self._duration_ms(started),
            )
            raise

    async def saga_reserve(self, tx_id: str, item_id: str, amount: int) -> dict:
        self._require_positive(amount, "amount")
        ok, retryable, detail = await self.handle_saga_command(
            action="reserve",
            tx_id=tx_id,
            payload={"item_id": item_id, "amount": int(amount)},
        )
        if not ok:
            raise HTTPException(status_code=400, detail=detail or "Saga reserve failed")
        return {"status": "done", "retryable": retryable}

    async def saga_release(self, tx_id: str, item_id: str, amount: int) -> dict:
        self._require_positive(amount, "amount")
        ok, retryable, detail = await self.handle_saga_command(
            action="release",
            tx_id=tx_id,
            payload={"item_id": item_id, "amount": int(amount)},
        )
        if not ok:
            raise HTTPException(status_code=400, detail=detail or "Saga release failed")
        return {"status": "done", "retryable": retryable}

    async def handle_saga_command(self, action: str, tx_id: str, payload: dict) -> tuple[bool, bool, str]:
        if action == "reserve":
            return await self._handle_saga_reserve(tx_id=tx_id, payload=payload)
        if action == "release":
            return await self._handle_saga_release(tx_id=tx_id, payload=payload)
        return False, False, f"Unsupported stock saga action: {action}"

    async def _handle_saga_reserve(self, tx_id: str, payload: dict) -> tuple[bool, bool, str]:
        item_id = str(payload.get("item_id", "")).strip()
        try:
            amount = int(payload.get("amount", 0) or 0)
        except (TypeError, ValueError):
            return False, False, "amount must be an integer"

        if not item_id:
            return False, False, "item_id is required"
        if amount <= 0:
            return False, False, "amount must be greater than zero"

        saga_key = f"saga:stock:tx:{tx_id}:item:{item_id}"
        max_watch_retries = 32

        for _ in range(max_watch_retries):
            try:
                async with self.repo.db.pipeline(transaction=True) as pipe:
                    await pipe.watch(item_id, saga_key)

                    existing_raw = await pipe.hgetall(saga_key)
                    if existing_raw:
                        existing = {
                            (k.decode() if isinstance(k, bytes) else str(k)): (
                                v.decode() if isinstance(v, bytes) else str(v)
                            )
                            for k, v in existing_raw.items()
                        }

                        try:
                            existing_amount = int(existing.get("amount", "0") or "0")
                        except ValueError:
                            await pipe.unwatch()
                            return False, False, "Corrupt saga record amount"

                        existing_state = existing.get("state", "")
                        if existing_amount != amount:
                            await pipe.unwatch()
                            return False, False, f"Saga reserve amount mismatch for tx={tx_id}, item={item_id}"
                        if existing_state == "reserved":
                            await pipe.unwatch()
                            return True, False, "already reserved"
                        if existing_state == "released":
                            await pipe.unwatch()
                            return False, False, f"Saga tx={tx_id}, item={item_id} already released"

                        await pipe.unwatch()
                        return False, False, f"Invalid saga stock state: {existing_state}"

                    item_raw = await pipe.get(item_id)
                    if item_raw is None:
                        await pipe.unwatch()
                        return False, False, f"Item: {item_id} not found!"

                    item = self.repo._decode_stock(item_raw)
                    if item.stock < amount:
                        await pipe.unwatch()
                        return False, False, f"Item: {item_id} insufficient stock (have {item.stock}, need {amount})"

                    item.stock -= amount
                    now_ms = str(int(time.time() * 1000))

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.hset(
                        saga_key,
                        mapping={
                            "tx_id": tx_id,
                            "item_id": item_id,
                            "amount": str(amount),
                            "state": "reserved",
                            "created_at_ms": now_ms,
                            "updated_at_ms": now_ms,
                        },
                    )
                    pipe.expire(saga_key, self.SAGA_KEY_TTL_SECONDS)
                    await pipe.execute()
                    self._log("saga_reserve_complete", tx_id=tx_id, item_id=item_id, amount=amount, status="reserved")
                    return True, False, "reserved"

            except WatchError:
                continue
            except HTTPException as exc:
                detail = str(exc.detail)
                return False, (detail == "DB error"), detail
            except RedisError as exc:
                self._log(
                    "saga_reserve_redis_error",
                    level="warning",
                    tx_id=tx_id,
                    item_id=item_id,
                    amount=amount,
                    detail=str(exc),
                )
                return False, True, "DB error"
            except Exception as exc:
                self._log(
                    "saga_reserve_failed",
                    level="warning",
                    tx_id=tx_id,
                    item_id=item_id,
                    amount=amount,
                    detail=str(exc),
                )
                return False, True, "Transient stock saga reserve error"

        return False, True, "Saga reserve contention timeout"


    async def _handle_saga_release(self, tx_id: str, payload: dict) -> tuple[bool, bool, str]:
        item_id = str(payload.get("item_id", "")).strip()
        try:
            amount = int(payload.get("amount", 0) or 0)
        except (TypeError, ValueError):
            return False, False, "amount must be an integer"

        if not item_id:
            return False, False, "item_id is required"
        if amount <= 0:
            return False, False, "amount must be greater than zero"

        saga_key = f"saga:stock:tx:{tx_id}:item:{item_id}"
        max_watch_retries = 32

        for _ in range(max_watch_retries):
            try:
                async with self.repo.db.pipeline(transaction=True) as pipe:
                    await pipe.watch(item_id, saga_key)

                    existing_raw = await pipe.hgetall(saga_key)
                    if not existing_raw:
                        now_ms = str(int(time.time() * 1000))
                        pipe.multi()
                        pipe.hset(
                            saga_key,
                            mapping={
                                "tx_id": tx_id,
                                "item_id": item_id,
                                "amount": str(amount),
                                "state": "released",
                                "created_at_ms": now_ms,
                                "updated_at_ms": now_ms,
                            },
                        )
                        pipe.expire(saga_key, self.SAGA_KEY_TTL_SECONDS)
                        await pipe.execute()
                        return True, False, "already released"

                    existing = {
                        (k.decode() if isinstance(k, bytes) else str(k)): (
                            v.decode() if isinstance(v, bytes) else str(v)
                        )
                        for k, v in existing_raw.items()
                    }

                    try:
                        existing_amount = int(existing.get("amount", "0") or "0")
                    except ValueError:
                        await pipe.unwatch()
                        return False, False, "Corrupt saga record amount"

                    existing_state = existing.get("state", "")
                    if existing_amount != amount:
                        await pipe.unwatch()
                        return False, False, f"Saga release amount mismatch for tx={tx_id}, item={item_id}"
                    if existing_state == "released":
                        await pipe.unwatch()
                        return True, False, "already released"
                    if existing_state != "reserved":
                        await pipe.unwatch()
                        return False, False, f"Invalid saga stock state: {existing_state}"

                    item_raw = await pipe.get(item_id)
                    if item_raw is None:
                        await pipe.unwatch()
                        return False, False, f"Item: {item_id} not found!"

                    item = self.repo._decode_stock(item_raw)
                    item.stock += amount
                    now_ms = str(int(time.time() * 1000))

                    pipe.multi()
                    pipe.set(item_id, msgpack.encode(item))
                    pipe.hset(
                        saga_key,
                        mapping={
                            "state": "released",
                            "updated_at_ms": now_ms,
                        },
                    )
                    pipe.expire(saga_key, self.SAGA_KEY_TTL_SECONDS)
                    await pipe.execute()

                    self._log("saga_release_complete", tx_id=tx_id, item_id=item_id, amount=amount, status="released")
                    return True, False, "released"

            except WatchError:
                continue
            except HTTPException as exc:
                detail = str(exc.detail)
                return False, (detail == "DB error"), detail
            except RedisError as exc:
                self._log(
                    "saga_release_redis_error",
                    level="warning",
                    tx_id=tx_id,
                    item_id=item_id,
                    amount=amount,
                    detail=str(exc),
                )
                return False, True, "DB error"
            except Exception as exc:
                self._log(
                    "saga_release_failed",
                    level="warning",
                    tx_id=tx_id,
                    item_id=item_id,
                    amount=amount,
                    detail=str(exc),
                )
                return False, True, "Transient stock saga release error"

        return False, True, "Saga release contention timeout"



    async def list_prepared_reservations(self):
        return await self.repo.list_prepared_reservations()

    @staticmethod
    def _require_positive(value: int, field_name: str):
        if int(value) <= 0:
            raise HTTPException(status_code=400, detail=f"{field_name} must be greater than zero")

    @staticmethod
    def _duration_ms(started: float) -> float:
        return round((time.perf_counter() - started) * 1000, 3)

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="stock-service",
            component="participant",
            **fields,
        )
