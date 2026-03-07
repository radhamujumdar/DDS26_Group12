import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable

from fastapi import HTTPException

from clients.payment_client import PaymentClient
from clients.stock_client import StockClient
from logging_utils import log_event
from models import OrderValue, ParticipantResult, REQ_ERROR_STR, SagaState, SagaTxRecord
from repository.order_repo import OrderRepository
from repository.saga_repo import SagaTxRepository


class SagaCoordinator:
    RETRY_LIMIT = 3
    RETRY_BACKOFF_SECONDS = 0.2
    IN_FLIGHT_WAIT_SECONDS = 5.0
    IN_FLIGHT_POLL_SECONDS = 0.1

    def __init__(
        self,
        stock_client: StockClient,
        payment_client: PaymentClient,
        saga_repo: SagaTxRepository,
        order_repo: OrderRepository,
        logger: logging.Logger,
    ):
        self.stock_client = stock_client
        self.payment_client = payment_client
        self.saga_repo = saga_repo
        self.order_repo = order_repo
        self.logger = logger

    async def checkout(self, order_id: str, order_entry: OrderValue):
        if order_entry.paid:
            self._log("checkout_skipped_already_paid", order_id=order_id)
            return

        tx, created = await self.saga_repo.get_or_create_by_order(
            order_id=order_id,
            user_id=order_entry.user_id,
            total_cost=order_entry.total_cost,
            items=self._aggregate_items(order_entry),
            state=SagaState.INIT,
        )
        self._log(
            "checkout_tx_selected",
            tx_id=tx.tx_id,
            order_id=order_id,
            created=created,
            tx_state=tx.state,
        )

        tx = await self._process_transaction(tx.tx_id)
        if tx.state != SagaState.COMPLETED.value:
            self._log(
                "checkout_failed",
                level="warning",
                tx_id=tx.tx_id,
                order_id=order_id,
                tx_state=tx.state,
                detail=tx.error or "Saga checkout failed",
            )
            raise HTTPException(status_code=400, detail=tx.error or "Saga checkout failed")

        self._log("checkout_completed", tx_id=tx.tx_id, order_id=order_id, tx_state=tx.state)

    async def recover_active_transactions(self):
        tx_ids = await self.saga_repo.list_active()
        self._log("recovery_scan", active_count=len(tx_ids))
        if not tx_ids:
            return

        for tx_id in tx_ids:
            try:
                tx = await self._process_transaction(tx_id, from_recovery=True)
            except HTTPException as exc:
                self._log(
                    "recovery_tx_failed",
                    level="warning",
                    tx_id=tx_id,
                    detail=str(exc.detail),
                )
            else:
                self._log("recovery_tx_processed", tx_id=tx.tx_id, order_id=tx.order_id, tx_state=tx.state)

    async def _process_transaction(self, tx_id: str, from_recovery: bool = False) -> SagaTxRecord:
        acquired = await self.saga_repo.acquire_tx_lock(tx_id)
        if not acquired:
            if from_recovery:
                tx = await self.saga_repo.get(tx_id)
                if tx is None:
                    raise HTTPException(status_code=400, detail="Saga transaction not found")
                return tx

            self._log("tx_lock_wait_begin", tx_id=tx_id)
            tx = await self._wait_for_in_flight_transaction(tx_id)
            if tx is not None:
                self._log("tx_lock_wait_end", tx_id=tx_id, tx_state=tx.state)
                return tx

            self._log("tx_lock_wait_timeout", level="warning", tx_id=tx_id)
            raise HTTPException(status_code=400, detail=f"Saga transaction {tx_id} is already in progress")

        try:
            tx = await self.saga_repo.get(tx_id)
            if tx is None:
                raise HTTPException(status_code=400, detail="Saga transaction not found")

            if tx.state == SagaState.COMPLETED.value:
                await self.order_repo.mark_paid(tx.order_id)
                await self.saga_repo.remove_active(tx.tx_id)
                return tx

            if tx.state in (SagaState.COMPENSATED.value, SagaState.FAILED.value):
                await self.saga_repo.remove_active(tx.tx_id)
                await self.saga_repo.clear_order_tx(tx.order_id)
                return tx

            if tx.state == SagaState.COMPENSATING.value:
                return await self._compensation_phase(tx)

            return await self._forward_phase(tx)
        finally:
            await self.saga_repo.release_tx_lock(tx_id)

    async def _forward_phase(self, tx: SagaTxRecord) -> SagaTxRecord:
        tx = await self._transition_state(tx, SagaState.RESERVING_STOCK)
        reserved_items = list(tx.stock_reserved_items)

        for item_id, amount in tx.items:
            if (item_id, amount) in reserved_items:
                continue

            result = await self._call_with_retries(
                operation_name="reserve",
                operation=lambda item_id=item_id, amount=amount: self.stock_client.saga_reserve_item(tx.tx_id, item_id, amount),
                tx=tx,
                phase="forward",
                participant="stock",
                item_id=item_id,
            )
            if not result.ok:
                tx = await self.saga_repo.update(
                    tx.tx_id,
                    state=SagaState.COMPENSATING.value,
                    error=result.detail or f"Stock reserve failed for item {item_id}",
                )
                return await self._compensation_phase(tx)

            reserved_items.append((item_id, amount))
            tx = await self.saga_repo.update(
                tx.tx_id,
                stock_reserved_items=reserved_items,
                attempts=tx.attempts + 1,
            )

        tx = await self._transition_state(tx, SagaState.STOCK_RESERVED)

        if not tx.payment_debited:
            tx = await self._transition_state(tx, SagaState.DEBITTING_PAYMENT)
            result = await self._call_with_retries(
                operation_name="debit",
                operation=lambda: self.payment_client.saga_debit(tx.tx_id, tx.user_id, tx.total_cost),
                tx=tx,
                phase="forward",
                participant="payment",
            )
            if not result.ok:
                tx = await self.saga_repo.update(
                    tx.tx_id,
                    state=SagaState.COMPENSATING.value,
                    error=result.detail or "Payment debit failed",
                )
                return await self._compensation_phase(tx)

            tx = await self.saga_repo.update(
                tx.tx_id,
                payment_debited=True,
                attempts=tx.attempts + 1,
            )

        tx = await self._transition_state(tx, SagaState.PAYMENT_DEBITED)
        tx = await self._transition_state(tx, SagaState.COMPLETED, error=None)
        await self.order_repo.mark_paid(tx.order_id)
        await self.saga_repo.remove_active(tx.tx_id)
        self._log("tx_terminal", tx_id=tx.tx_id, order_id=tx.order_id, tx_state=tx.state)
        return tx

    async def _compensation_phase(self, tx: SagaTxRecord) -> SagaTxRecord:
        tx = await self._transition_state(tx, SagaState.COMPENSATING, error=tx.error)

        if tx.payment_debited and not tx.payment_refunded:
            result = await self._call_with_retries(
                operation_name="refund",
                operation=lambda: self.payment_client.saga_refund(tx.tx_id),
                tx=tx,
                phase="compensation",
                participant="payment",
            )
            if not result.ok:
                return await self.saga_repo.update(
                    tx.tx_id,
                    state=SagaState.COMPENSATING.value,
                    error=result.detail or "Payment compensation failed",
                )
            tx = await self.saga_repo.update(
                tx.tx_id,
                payment_refunded=True,
                attempts=tx.attempts + 1,
            )

        released_items = list(tx.stock_released_items)
        for item_id, amount in reversed(tx.stock_reserved_items):
            if (item_id, amount) in released_items:
                continue

            result = await self._call_with_retries(
                operation_name="release",
                operation=lambda item_id=item_id, amount=amount: self.stock_client.saga_release_item(tx.tx_id, item_id, amount),
                tx=tx,
                phase="compensation",
                participant="stock",
                item_id=item_id,
            )
            if not result.ok:
                return await self.saga_repo.update(
                    tx.tx_id,
                    state=SagaState.COMPENSATING.value,
                    error=result.detail or f"Stock compensation failed for item {item_id}",
                )

            released_items.append((item_id, amount))
            tx = await self.saga_repo.update(
                tx.tx_id,
                stock_released_items=released_items,
                attempts=tx.attempts + 1,
            )

        tx = await self._transition_state(tx, SagaState.COMPENSATED, error=tx.error)
        await self.saga_repo.remove_active(tx.tx_id)
        await self.saga_repo.clear_order_tx(tx.order_id)
        self._log(
            "tx_terminal",
            tx_id=tx.tx_id,
            order_id=tx.order_id,
            tx_state=tx.state,
            detail=tx.error,
        )
        return tx

    async def _call_with_retries(
        self,
        operation_name: str,
        operation: Callable[[], Awaitable[ParticipantResult]],
        tx: SagaTxRecord,
        phase: str,
        participant: str,
        item_id: str | None = None,
    ) -> ParticipantResult:
        for attempt in range(1, self.RETRY_LIMIT + 1):
            started = time.perf_counter()
            try:
                result = await operation()
            except HTTPException as exc:
                detail = str(exc.detail)
                retryable = detail == REQ_ERROR_STR
                result = ParticipantResult(ok=False, retryable=retryable, detail=detail)

            duration_ms = round((time.perf_counter() - started) * 1000, 3)
            self._log(
                "participant_attempt",
                level="info" if result.ok else "warning",
                tx_id=tx.tx_id,
                order_id=tx.order_id,
                phase=phase,
                participant=participant,
                operation=operation_name,
                item_id=item_id,
                attempt=attempt,
                ok=result.ok,
                retryable=result.retryable,
                detail=result.detail,
                duration_ms=duration_ms,
            )

            if result.ok:
                return result
            if not result.retryable or attempt == self.RETRY_LIMIT:
                return result
            await asyncio.sleep(self.RETRY_BACKOFF_SECONDS * attempt)

        return ParticipantResult(ok=False, detail=f"{operation_name} failed")

    async def _wait_for_in_flight_transaction(self, tx_id: str) -> SagaTxRecord | None:
        deadline = asyncio.get_running_loop().time() + self.IN_FLIGHT_WAIT_SECONDS
        while asyncio.get_running_loop().time() < deadline:
            tx = await self.saga_repo.get(tx_id)
            if tx is None:
                return None
            if tx.state in (
                SagaState.COMPLETED.value,
                SagaState.COMPENSATED.value,
                SagaState.FAILED.value,
            ):
                return tx
            await asyncio.sleep(self.IN_FLIGHT_POLL_SECONDS)
        return None

    async def _transition_state(
        self,
        tx: SagaTxRecord,
        next_state: SagaState,
        error: str | None = None,
    ) -> SagaTxRecord:
        prev_state = tx.state
        updated = await self.saga_repo.update(tx.tx_id, state=next_state.value, error=error)
        self._log(
            "tx_state_transition",
            tx_id=updated.tx_id,
            order_id=updated.order_id,
            from_state=prev_state,
            to_state=updated.state,
            detail=error,
        )
        return updated

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="order-service",
            component="saga",
            **fields,
        )

    @staticmethod
    def _aggregate_items(order_entry: OrderValue) -> list[tuple[str, int]]:
        totals: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            totals[item_id] += quantity
        return [(item_id, qty) for item_id, qty in totals.items()]
