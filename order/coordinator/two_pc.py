import logging
from collections import defaultdict

from fastapi import HTTPException

from clients.payment_client import PaymentClient
from clients.stock_client import StockClient
from models import OrderValue, TxState
from repository.tx_repo import TxRepository


class TwoPCCoordinator:
    def __init__(
        self,
        stock_client: StockClient,
        payment_client: PaymentClient,
        tx_repo: TxRepository,
        logger: logging.Logger,
    ):
        self.stock_client = stock_client
        self.payment_client = payment_client
        self.tx_repo = tx_repo
        self.logger = logger

    async def checkout(self, order_id: str, order_entry: OrderValue):
        tx = await self.tx_repo.create(order_id=order_id, state=TxState.INIT)
        self.logger.debug("Starting checkout for order=%s tx=%s", order_id, tx.tx_id)
        await self.tx_repo.update_state(tx.tx_id, TxState.PREPARING)

        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity

        removed_items: list[tuple[str, int]] = []
        for item_id, quantity in items_quantities.items():
            stock_reply = await self.stock_client.subtract(item_id, quantity)
            if stock_reply.status_code != 200:
                await self._rollback_stock(removed_items)
                await self.tx_repo.update_state(tx.tx_id, TxState.ABORTED, error=f"Out of stock on {item_id}")
                await self.tx_repo.remove_active(tx.tx_id)
                raise HTTPException(status_code=400, detail=f"Out of stock on item_id: {item_id}")
            removed_items.append((item_id, quantity))

        await self.tx_repo.update_state(tx.tx_id, TxState.COMMITTING)
        payment_reply = await self.payment_client.pay(order_entry.user_id, order_entry.total_cost)
        if payment_reply.status_code != 200:
            await self._rollback_stock(removed_items)
            await self.tx_repo.update_state(tx.tx_id, TxState.ABORTED, error="User out of credit")
            await self.tx_repo.remove_active(tx.tx_id)
            raise HTTPException(status_code=400, detail="User out of credit")

        await self.tx_repo.update_state(tx.tx_id, TxState.COMMITTED)
        await self.tx_repo.remove_active(tx.tx_id)
        self.logger.debug("Checkout successful for order=%s tx=%s", order_id, tx.tx_id)

    async def _rollback_stock(self, removed_items: list[tuple[str, int]]):
        for item_id, quantity in removed_items:
            await self.stock_client.add(item_id, quantity)
