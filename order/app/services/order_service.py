from __future__ import annotations

from dataclasses import dataclass

from shop_common.checkout import CheckoutOrder

from ..clients.stock_client import StockGatewayClient
from ..repositories.order_repository import OrderRepository


@dataclass(frozen=True, slots=True)
class CheckoutPreparation:
    order: CheckoutOrder
    attempt_no: int | None
    already_paid: bool = False


class OrderService:
    def __init__(
        self,
        repository: OrderRepository,
        stock_client: StockGatewayClient | None = None,
    ) -> None:
        self._repository = repository
        self._stock_client = stock_client

    async def create_order(self, user_id: str) -> str:
        return await self._repository.create_order(user_id)

    async def batch_init(
        self,
        *,
        count: int,
        n_items: int,
        n_users: int,
        item_price: int,
    ) -> None:
        await self._repository.batch_init(
            count=count,
            n_items=n_items,
            n_users=n_users,
            item_price=item_price,
        )

    async def find_order(self, order_id: str) -> dict[str, object]:
        order_entry = await self._repository.get_order(order_id)
        return {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }

    async def add_item(
        self,
        order_id: str,
        *,
        item_id: str,
        quantity: int,
    ) -> int:
        if self._stock_client is None:
            raise RuntimeError("OrderService.add_item() requires a configured stock client.")
        item = await self._stock_client.find_item(item_id)
        order_entry = await self._repository.add_item(
            order_id,
            item_id=item_id,
            quantity=quantity,
            item_price=item.price,
        )
        return order_entry.total_cost

    async def load_checkout_order(self, order_id: str) -> CheckoutOrder:
        return await self._repository.load_checkout_order(order_id)

    async def prepare_checkout(self, order_id: str) -> CheckoutPreparation:
        order, attempt_no, already_paid = await self._repository.prepare_checkout_attempt(
            order_id
        )
        return CheckoutPreparation(
            order=order,
            attempt_no=attempt_no,
            already_paid=already_paid,
        )

    async def complete_checkout_attempt(
        self,
        order_id: str,
        *,
        attempt_no: int,
        status: str,
    ) -> None:
        await self._repository.complete_checkout_attempt(
            order_id,
            attempt_no=attempt_no,
            status=status,
        )

    async def mark_order_paid(
        self,
        order_id: str,
        *,
        activity_execution_id: str,
    ) -> CheckoutOrder:
        return await self._repository.mark_order_paid_idempotent(
            order_id,
            activity_execution_id=activity_execution_id,
        )
