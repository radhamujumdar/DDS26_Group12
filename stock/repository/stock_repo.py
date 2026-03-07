import redis.asyncio as redis
from fastapi import HTTPException
from msgspec import DecodeError, msgpack
from redis.asyncio import Redis
from redis.exceptions import RedisError, WatchError

from models import DB_ERROR_STR, Reservation, ReservationState, StockValue


def reservation_key(tx_id: str, item_id: str) -> str:
    return f"txn:{tx_id}:{item_id}"


class StockRepository:
    def __init__(self, db: Redis):
        self.db = db

    async def create_item(self, item_id: str, price: int):
        value = msgpack.encode(StockValue(stock=0, price=int(price)))
        try:
            await self.db.set(item_id, value)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def batch_init_items(self, kv_pairs: dict[str, bytes]):
        try:
            await self.db.mset(kv_pairs)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def get_item(self, item_id: str) -> StockValue | None:
        try:
            raw = await self.db.get(item_id)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return self._decode_stock(raw)

    async def get_item_or_error(self, item_id: str) -> StockValue:
        item = await self.get_item(item_id)
        if item is None:
            raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")
        return item

    async def save_item(self, item_id: str, item: StockValue):
        try:
            await self.db.set(item_id, msgpack.encode(item))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def add_stock_non_atomic(self, item_id: str, amount: int) -> int:
        item = await self.get_item_or_error(item_id)
        item.stock += int(amount)
        await self.save_item(item_id, item)
        return item.stock

    async def subtract_stock_non_atomic(self, item_id: str, amount: int) -> int:
        item = await self.get_item_or_error(item_id)
        item.stock -= int(amount)
        if item.stock < 0:
            raise HTTPException(status_code=400, detail=f"Item: {item_id} stock cannot get reduced below zero!")
        await self.save_item(item_id, item)
        return item.stock

    async def get_reservation(self, tx_id: str, item_id: str) -> Reservation | None:
        rkey = reservation_key(tx_id, item_id)
        try:
            raw = await self.db.get(rkey)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        if raw is None:
            return None
        return self._decode_reservation(raw)

    async def save_reservation(self, reservation: Reservation):
        rkey = reservation_key(reservation.tx_id, reservation.item_id)
        try:
            await self.db.set(rkey, msgpack.encode(reservation))
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def list_prepared_reservations(self) -> list[Reservation]:
        prepared: list[Reservation] = []
        try:
            async for raw_key in self.db.scan_iter(match="txn:*:*"):
                key = raw_key.decode() if isinstance(raw_key, bytes) else str(raw_key)
                if key.count(":") != 2:
                    continue
                raw_record = await self.db.get(key)
                if raw_record is None:
                    continue
                reservation = self._decode_reservation(raw_record)
                if reservation.state == ReservationState.PREPARED:
                    prepared.append(reservation)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
        return prepared

    async def prepare_reservation(self, tx_id: str, item_id: str, amount: int) -> str:
        rkey = reservation_key(tx_id, item_id)
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(item_id, rkey)
                        existing_raw = await pipe.get(rkey)
                        if existing_raw is not None:
                            existing = self._decode_reservation(existing_raw)
                            if int(existing.amount) != int(amount):
                                await pipe.unwatch()
                                raise HTTPException(
                                    status_code=400,
                                    detail=f"Txn {tx_id}: prepare amount mismatch for item {item_id}",
                                )
                            if existing.state == ReservationState.PREPARED:
                                await pipe.unwatch()
                                return ReservationState.PREPARED.value
                            if existing.state == ReservationState.COMMITTED:
                                await pipe.unwatch()
                                return "already committed"
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Txn {tx_id}: previously aborted for item {item_id}",
                            )

                        item_raw = await pipe.get(item_id)
                        if item_raw is None:
                            await pipe.unwatch()
                            raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")
                        item = self._decode_stock(item_raw)
                        if item.stock < amount:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Item: {item_id} insufficient stock (have {item.stock}, need {amount})",
                            )
                        item.stock -= amount
                        reservation = Reservation(
                            tx_id=tx_id,
                            item_id=item_id,
                            amount=amount,
                            state=ReservationState.PREPARED,
                        )
                        pipe.multi()
                        pipe.set(item_id, msgpack.encode(item))
                        pipe.set(rkey, msgpack.encode(reservation))
                        await pipe.execute()
                        return ReservationState.PREPARED.value
                    except WatchError:
                        continue
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def commit_reservation(self, tx_id: str, item_id: str, amount: int) -> str:
        rkey = reservation_key(tx_id, item_id)
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(rkey)
                        raw = await pipe.get(rkey)
                        if raw is None:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Unknown transaction {tx_id} for item {item_id}",
                            )
                        reservation = self._decode_reservation(raw)
                        if int(amount) != int(reservation.amount):
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Txn {tx_id}: commit amount mismatch for item {item_id}",
                            )
                        if reservation.state == ReservationState.COMMITTED:
                            await pipe.unwatch()
                            return ReservationState.COMMITTED.value
                        if reservation.state == ReservationState.ABORTED:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Txn {tx_id}: cannot commit, already aborted for item {item_id}",
                            )
                        if reservation.state != ReservationState.PREPARED:
                            await pipe.unwatch()
                            raise HTTPException(
                                status_code=400,
                                detail=f"Txn {tx_id}: invalid state {reservation.state.value} for item {item_id}",
                            )
                        updated = Reservation(
                            tx_id=reservation.tx_id,
                            item_id=reservation.item_id,
                            amount=reservation.amount,
                            state=ReservationState.COMMITTED,
                        )
                        pipe.multi()
                        pipe.set(rkey, msgpack.encode(updated))
                        await pipe.execute()
                        return ReservationState.COMMITTED.value
                    except WatchError:
                        continue
        except HTTPException:
            raise
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    async def abort_reservation(self, tx_id: str, item_id: str, amount: int) -> str:
        rkey = reservation_key(tx_id, item_id)
        try:
            raw = await self.db.get(rkey)
        except RedisError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

        if raw is None:
            return ReservationState.ABORTED.value

        reservation = self._decode_reservation(raw)
        if int(amount) != int(reservation.amount):
            raise HTTPException(
                status_code=400,
                detail=f"Txn {tx_id}: abort amount mismatch for item {item_id}",
            )
        if reservation.state == ReservationState.ABORTED:
            return ReservationState.ABORTED.value
        if reservation.state == ReservationState.COMMITTED:
            raise HTTPException(
                status_code=400,
                detail=f"Txn {tx_id}: cannot abort, already committed for item {item_id}",
            )

        success = await self._do_abort(tx_id=tx_id, item_id=item_id)
        if not success:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)
        return ReservationState.ABORTED.value

    async def _do_abort(self, tx_id: str, item_id: str) -> bool:
        rkey = reservation_key(tx_id, item_id)
        try:
            async with self.db.pipeline(transaction=True) as pipe:
                while True:
                    try:
                        await pipe.watch(item_id, rkey)
                        res_raw = await pipe.get(rkey)
                        if res_raw is None:
                            return True

                        current_res = self._decode_reservation(res_raw)
                        if current_res.state == ReservationState.ABORTED:
                            return True
                        if current_res.state == ReservationState.COMMITTED:
                            return False

                        item_raw = await pipe.get(item_id)
                        pipe.multi()
                        if item_raw is not None:
                            item = self._decode_stock(item_raw)
                            item.stock += current_res.amount
                            pipe.set(item_id, msgpack.encode(item))

                        updated = Reservation(
                            tx_id=current_res.tx_id,
                            item_id=current_res.item_id,
                            amount=current_res.amount,
                            state=ReservationState.ABORTED,
                        )
                        pipe.set(rkey, msgpack.encode(updated))
                        await pipe.execute()
                        return True
                    except WatchError:
                        continue
        except RedisError:
            return False

    @staticmethod
    def _decode_stock(raw: bytes) -> StockValue:
        try:
            return msgpack.decode(raw, type=StockValue)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc

    @staticmethod
    def _decode_reservation(raw: bytes) -> Reservation:
        try:
            return msgpack.decode(raw, type=Reservation)
        except DecodeError as exc:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR) from exc
