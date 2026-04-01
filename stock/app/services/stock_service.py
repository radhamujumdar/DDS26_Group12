from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fluxi_sdk import activity
from shop_common.checkout import StockReservation, StockUnavailableError

from ..domain.errors import StockInsufficientError, StockItemNotFoundError
from ..repositories.stock_repository import StockRepository


class StockService:
    def __init__(self, repository: StockRepository) -> None:
        self._repository = repository

    async def create_item(self, price: int) -> str:
        return await self._repository.create_item(price)

    async def batch_init(self, *, count: int, starting_stock: int, item_price: int) -> None:
        await self._repository.batch_init(
            count=count,
            starting_stock=starting_stock,
            item_price=item_price,
        )

    async def find_item(self, item_id: str) -> dict[str, int]:
        item = await self._repository.get_item(item_id)
        return {"stock": item.stock, "price": item.price}

    async def add_stock(self, item_id: str, amount: int) -> int:
        item = await self._repository.add_stock(item_id, amount)
        return item.stock

    async def subtract_stock(self, item_id: str, amount: int) -> int:
        item = await self._repository.subtract_stock(item_id, amount)
        return item.stock

    async def reserve_stock(
        self,
        item_id: str,
        quantity: int,
        *,
        activity_execution_id: str,
    ) -> StockReservation:
        try:
            return await self._repository.reserve_stock_idempotent(
                item_id,
                quantity=quantity,
                activity_execution_id=activity_execution_id,
            )
        except (StockItemNotFoundError, StockInsufficientError) as exc:
            raise StockUnavailableError(str(exc)) from exc

    async def release_stock(
        self,
        item_id: str,
        quantity: int,
        *,
        activity_execution_id: str,
    ) -> StockReservation:
        try:
            return await self._repository.release_stock_idempotent(
                item_id,
                quantity=quantity,
                activity_execution_id=activity_execution_id,
            )
        except StockItemNotFoundError as exc:
            raise StockUnavailableError(str(exc)) from exc


def create_stock_activities(
    stock_service: StockService,
) -> tuple[Callable[..., Any], Callable[..., Any]]:
    @activity.defn(name="reserve_stock")
    async def reserve_stock(item_id: str, quantity: int) -> StockReservation:
        return await stock_service.reserve_stock(
            item_id,
            quantity,
            activity_execution_id=activity.info().activity_execution_id,
        )

    @activity.defn(name="release_stock")
    async def release_stock(item_id: str, quantity: int) -> StockReservation:
        return await stock_service.release_stock(
            item_id,
            quantity,
            activity_execution_id=activity.info().activity_execution_id,
        )

    return reserve_stock, release_stock
