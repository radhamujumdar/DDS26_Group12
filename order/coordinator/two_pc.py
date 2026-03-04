import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable

from fastapi import HTTPException

from clients.payment_client import PaymentClient
from clients.stock_client import StockClient
from models import OrderValue, ParticipantResult, REQ_ERROR_STR, TxRecord, TxState
from repository.order_repo import OrderRepository
from repository.tx_repo import TxRepository


class TwoPCCoordinator:
    RETRY_LIMIT = 3
    RETRY_BACKOFF_SECONDS = 0.2
    IN_FLIGHT_WAIT_SECONDS = 5.0
    IN_FLIGHT_POLL_SECONDS = 0.1

    def __init__(
        self,
        stock_client: StockClient,
        payment_client: PaymentClient,
        tx_repo: TxRepository,
        order_repo: OrderRepository,
        logger: logging.Logger,
    ):
        self.stock_client = stock_client
        self.payment_client = payment_client
        self.tx_repo = tx_repo
        self.order_repo = order_repo
        self.logger = logger

    async def checkout(self, order_id: str, order_entry: OrderValue):
        if order_entry.paid:
            return

        tx, created = await self.tx_repo.get_or_create_by_order(
            order_id=order_id,
            user_id=order_entry.user_id,
            total_cost=order_entry.total_cost,
            items=self._aggregate_items(order_entry),
            state=TxState.INIT,
        )
        if created:
            self.logger.info("Created tx=%s for order=%s", tx.tx_id, order_id)
        else:
            self.logger.info("Reusing tx=%s for order=%s state=%s", tx.tx_id, order_id, tx.state)

        tx = await self._process_transaction(tx.tx_id, from_recovery=False)
        if tx.state != TxState.COMMITTED.value:
            raise HTTPException(status_code=400, detail=tx.error or "Checkout failed")

    async def recover_active_transactions(self):
        tx_ids = await self.tx_repo.list_active()
        if not tx_ids:
            return
        self.logger.info("Recovering %s active transaction(s)", len(tx_ids))
        for tx_id in tx_ids:
            try:
                await self._process_transaction(tx_id, from_recovery=True)
            except HTTPException as exc:
                self.logger.warning("Recovery for tx=%s failed with detail=%s", tx_id, exc.detail)

    async def _process_transaction(self, tx_id: str, from_recovery: bool) -> TxRecord:
        acquired = await self.tx_repo.acquire_tx_lock(tx_id)
        if not acquired:
            if from_recovery:
                tx = await self.tx_repo.get(tx_id)
                if tx is None:
                    raise HTTPException(status_code=400, detail="Transaction not found")
                return tx
            tx = await self._wait_for_in_flight_transaction(tx_id)
            if tx is not None:
                return tx
            raise HTTPException(status_code=400, detail=f"Transaction {tx_id} is already in progress")

        try:
            tx = await self.tx_repo.get(tx_id)
            if tx is None:
                raise HTTPException(status_code=400, detail="Transaction not found")

            if tx.state == TxState.COMMITTED.value:
                await self.order_repo.mark_paid(tx.order_id)
                await self.tx_repo.remove_active(tx.tx_id)
                return tx
            if tx.state == TxState.ABORTED.value:
                await self.tx_repo.remove_active(tx.tx_id)
                return tx

            if tx.state in (TxState.INIT.value, TxState.PREPARING.value):
                tx = await self._prepare_phase(tx)
            if tx.state in (TxState.PREPARED.value, TxState.COMMITTING.value):
                tx = await self._commit_phase(tx)
            if tx.state == TxState.ABORTING.value:
                tx = await self._abort_phase(tx)

            return tx
        finally:
            await self.tx_repo.release_tx_lock(tx_id)

    async def _prepare_phase(self, tx: TxRecord) -> TxRecord:
        tx = await self.tx_repo.update(tx.tx_id, state=TxState.PREPARING.value, error=None)
        prepared_items = list(tx.stock_prepared_items)

        for item_id, amount in tx.items:
            if (item_id, amount) in prepared_items:
                continue
            result = await self._call_with_retries(
                f"stock prepare {item_id}",
                lambda item_id=item_id, amount=amount: self.stock_client.prepare_item(tx.tx_id, item_id, amount),
            )
            if not result.ok:
                return await self._start_abort(tx.tx_id, result.detail or f"Out of stock on item_id: {item_id}")
            prepared_items.append((item_id, amount))
            tx = await self.tx_repo.update(
                tx.tx_id,
                stock_prepared_items=prepared_items,
                attempts=tx.attempts + 1,
            )

        if not tx.payment_prepared:
            result = await self._call_with_retries(
                "payment prepare",
                lambda: self.payment_client.prepare(tx.tx_id, tx.user_id, tx.total_cost),
            )
            if not result.ok:
                return await self._start_abort(tx.tx_id, result.detail or "User out of credit")
            tx = await self.tx_repo.update(
                tx.tx_id,
                payment_prepared=True,
                attempts=tx.attempts + 1,
            )

        tx = await self.tx_repo.update(tx.tx_id, state=TxState.PREPARED.value, error=None)
        self.logger.info("tx=%s reached PREPARED", tx.tx_id)
        return tx

    async def _commit_phase(self, tx: TxRecord) -> TxRecord:
        tx = await self.tx_repo.update(tx.tx_id, state=TxState.COMMITTING.value, error=None)
        committed_items = list(tx.stock_committed_items)

        for item_id, amount in tx.items:
            if (item_id, amount) in committed_items:
                continue
            result = await self._call_with_retries(
                f"stock commit {item_id}",
                lambda item_id=item_id, amount=amount: self.stock_client.commit_item(tx.tx_id, item_id, amount),
            )
            if not result.ok:
                return await self._start_abort(tx.tx_id, result.detail or f"Stock commit failed for item {item_id}")
            committed_items.append((item_id, amount))
            tx = await self.tx_repo.update(
                tx.tx_id,
                stock_committed_items=committed_items,
                attempts=tx.attempts + 1,
            )

        if not tx.payment_committed:
            result = await self._call_with_retries(
                "payment commit",
                lambda: self.payment_client.commit(tx.tx_id),
            )
            if not result.ok:
                return await self._start_abort(tx.tx_id, result.detail or "Payment commit failed")
            tx = await self.tx_repo.update(
                tx.tx_id,
                payment_committed=True,
                attempts=tx.attempts + 1,
            )

        tx = await self.tx_repo.update(tx.tx_id, state=TxState.COMMITTED.value, error=None)
        await self.order_repo.mark_paid(tx.order_id)
        await self.tx_repo.remove_active(tx.tx_id)
        self.logger.info("tx=%s committed successfully", tx.tx_id)
        return tx

    async def _abort_phase(self, tx: TxRecord) -> TxRecord:
        tx = await self.tx_repo.update(tx.tx_id, state=TxState.ABORTING.value)

        for item_id, amount in tx.stock_prepared_items:
            result = await self._call_with_retries(
                f"stock abort {item_id}",
                lambda item_id=item_id, amount=amount: self.stock_client.abort_item(tx.tx_id, item_id, amount),
            )
            if not result.ok:
                tx = await self.tx_repo.update(
                    tx.tx_id,
                    state=TxState.ABORTING.value,
                    error=result.detail or "Stock rollback failed",
                )
                return tx

        if tx.payment_prepared and not tx.payment_committed:
            result = await self._call_with_retries(
                "payment abort",
                lambda: self.payment_client.abort(tx.tx_id),
            )
            if not result.ok:
                tx = await self.tx_repo.update(
                    tx.tx_id,
                    state=TxState.ABORTING.value,
                    error=result.detail or "Payment rollback failed",
                )
                return tx

        tx = await self.tx_repo.update(tx.tx_id, state=TxState.ABORTED.value)
        await self.tx_repo.remove_active(tx.tx_id)
        await self.tx_repo.clear_order_tx(tx.order_id)
        self.logger.info("tx=%s aborted", tx.tx_id)
        return tx

    async def _start_abort(self, tx_id: str, error_detail: str) -> TxRecord:
        tx = await self.tx_repo.update(tx_id, state=TxState.ABORTING.value, error=error_detail)
        return await self._abort_phase(tx)

    async def _call_with_retries(
        self,
        operation_name: str,
        operation: Callable[[], Awaitable[ParticipantResult]],
    ) -> ParticipantResult:
        for attempt in range(1, self.RETRY_LIMIT + 1):
            try:
                result = await operation()
            except HTTPException as exc:
                detail = str(exc.detail)
                retryable = detail == REQ_ERROR_STR
                result = ParticipantResult(ok=False, retryable=retryable, detail=detail)

            if result.ok:
                return result
            if not result.retryable or attempt == self.RETRY_LIMIT:
                self.logger.warning(
                    "Operation '%s' failed on attempt=%s detail=%s",
                    operation_name,
                    attempt,
                    result.detail,
                )
                return result
            await asyncio.sleep(self.RETRY_BACKOFF_SECONDS * attempt)

        return ParticipantResult(ok=False, detail=f"{operation_name} failed")

    async def _wait_for_in_flight_transaction(self, tx_id: str) -> TxRecord | None:
        deadline = asyncio.get_running_loop().time() + self.IN_FLIGHT_WAIT_SECONDS
        while asyncio.get_running_loop().time() < deadline:
            tx = await self.tx_repo.get(tx_id)
            if tx is None:
                return None
            if tx.state in (TxState.COMMITTED.value, TxState.ABORTED.value):
                return tx
            await asyncio.sleep(self.IN_FLIGHT_POLL_SECONDS)
        return None

    @staticmethod
    def _aggregate_items(order_entry: OrderValue) -> list[tuple[str, int]]:
        totals: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            totals[item_id] += quantity
        return [(item_id, qty) for item_id, qty in totals.items()]
