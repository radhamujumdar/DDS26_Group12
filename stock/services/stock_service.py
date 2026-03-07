import logging
import time
import uuid

from fastapi import HTTPException
from msgspec import msgpack

from logging_utils import log_event
from models import ReservationState, StockValue
from repository.stock_repo import StockRepository


class StockService:
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
