import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable

from fastapi import HTTPException

from clients.payment_client import PaymentClient
from clients.stock_client import StockClient
from logging_utils import log_event
from models import OrderValue, ParticipantResult, REQ_ERROR_STR, TxRecord, TxState
from repository.order_repo import OrderRepository
from repository.tx_repo import TxRepository


class TwoPCCoordinator:
    RETRY_LIMIT = 3
    RETRY_BACKOFF_SECONDS = 0.2
    IN_FLIGHT_WAIT_SECONDS = 60.0
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

        tx = await self._process_transaction(tx.tx_id, from_recovery=False)
        if tx.state != TxState.COMMITTED.value:
            raise HTTPException(status_code=400, detail=tx.error or "Checkout failed")

    async def recover_active_transactions(self):
        tx_ids = await self.tx_repo.list_active()
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

    async def _process_transaction(self, tx_id: str, from_recovery: bool) -> TxRecord:
        acquired = await self.tx_repo.acquire_tx_lock(tx_id)
        if not acquired:
            if from_recovery:
                tx = await self.tx_repo.get(tx_id)
                if tx is None:
                    raise HTTPException(status_code=400, detail="Transaction not found")
                return tx

            self._log("tx_lock_wait_begin", tx_id=tx_id)
            tx = await self._wait_for_in_flight_transaction(tx_id)
            if tx is not None:
                self._log("tx_lock_wait_end", tx_id=tx_id, tx_state=tx.state)
                return tx

            self._log("tx_lock_wait_timeout", level="warning", tx_id=tx_id)
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
        prepared_set = set(tx.stock_prepared_items)
        payment_was_prepared = tx.payment_prepared

        # Prepare all stock items + payment in parallel
        items_to_prepare = [(iid, amt) for iid, amt in tx.items if (iid, amt) not in prepared_set]
        need_payment = not tx.payment_prepared

        tasks = []
        for item_id, amount in items_to_prepare:
            async def _prepare_stock(iid=item_id, a=amount):
                r = await self._call_with_retries(
                    operation_name="prepare",
                    operation=lambda iid=iid, a=a: self.stock_client.prepare_item(tx.tx_id, iid, a),
                    tx=tx, phase="prepare", participant="stock", item_id=iid,
                )
                return ("stock", iid, a, r)
            tasks.append(_prepare_stock())

        if need_payment:
            async def _prepare_pay():
                r = await self._call_with_retries(
                    operation_name="prepare",
                    operation=lambda: self.payment_client.prepare(tx.tx_id, tx.user_id, tx.total_cost),
                    tx=tx, phase="prepare", participant="payment",
                )
                return ("payment", None, None, r)
            tasks.append(_prepare_pay())

        if tasks:
            results = await asyncio.gather(*tasks)
            abort_detail = None

            for result_tuple in results:
                kind, iid, amt, result = result_tuple
                if result.ok:
                    if kind == "stock":
                        prepared_set.add((iid, amt))
                    elif kind == "payment":
                        payment_was_prepared = True
                else:
                    if abort_detail is None:
                        if kind == "stock":
                            abort_detail = result.detail or f"Out of stock on item_id: {iid}"
                        else:
                            abort_detail = result.detail or "User out of credit"

            if abort_detail:
                # Save any successful prepares before aborting
                update_kwargs = {}
                if prepared_set != set(tx.stock_prepared_items):
                    update_kwargs["stock_prepared_items"] = list(prepared_set)
                if payment_was_prepared != tx.payment_prepared:
                    update_kwargs["payment_prepared"] = payment_was_prepared
                
                if update_kwargs:
                    await self.tx_repo.update(tx.tx_id, **update_kwargs)
                return await self._start_abort(tx.tx_id, abort_detail)

            # Single write: save prepare results + transition to PREPARED
            update_kwargs = {
                "stock_prepared_items": list(prepared_set),
                "payment_prepared": payment_was_prepared,
                "state": TxState.PREPARED.value,
                "attempts": tx.attempts + len(tasks),
            }
            tx = await self.tx_repo.update(tx.tx_id, **update_kwargs)
        else:
            tx = await self._transition_state(tx, TxState.PREPARED)

        return tx

    async def _commit_phase(self, tx: TxRecord) -> TxRecord:
        committed_set = set(tx.stock_committed_items)
        payment_committed = tx.payment_committed

        # Commit all stock items in parallel + payment commit
        items_to_commit = [(iid, amt) for iid, amt in tx.items if (iid, amt) not in committed_set]

        async def _commit_stock(item_id: str, amount: int) -> tuple[str, int, ParticipantResult]:
            r = await self._call_with_retries(
                operation_name="commit",
                operation=lambda iid=item_id, a=amount: self.stock_client.commit_item(tx.tx_id, iid, a),
                tx=tx, phase="commit", participant="stock", item_id=item_id,
            )
            return item_id, amount, r

        # Run stock commits and payment commit in parallel
        tasks = [_commit_stock(iid, amt) for iid, amt in items_to_commit]
        if not payment_committed:
            async def _commit_payment() -> ParticipantResult:
                return await self._call_with_retries(
                    operation_name="commit",
                    operation=lambda: self.payment_client.commit(tx.tx_id),
                    tx=tx, phase="commit", participant="payment",
                )
            tasks.append(_commit_payment())

        results = await asyncio.gather(*tasks)

        # Process results
        error_detail = None
        for r in results:
            if isinstance(r, tuple):
                item_id, amount, result = r
                if result.ok:
                    committed_set.add((item_id, amount))
                else:
                    if error_detail is None:
                        error_detail = result.detail or f"Stock commit failed for item {item_id}"
            else:
                if r.ok:
                    payment_committed = True
                else:
                    if error_detail is None:
                        error_detail = r.detail or "Payment commit failed"

        if error_detail:
            update_kwargs = {
                "state": TxState.COMMITTING.value,
                "error": error_detail,
                "attempts": tx.attempts + len(tasks),
            }
            if committed_set != set(tx.stock_committed_items):
                update_kwargs["stock_committed_items"] = list(committed_set)
            if payment_committed != tx.payment_committed:
                update_kwargs["payment_committed"] = payment_committed
            
            return await self.tx_repo.update(tx.tx_id, **update_kwargs)

        # Single write: save commit results + transition to COMMITTED
        tx = await self.tx_repo.update(
            tx.tx_id,
            stock_committed_items=list(committed_set),
            payment_committed=True,
            state=TxState.COMMITTED.value,
            error=None,
            attempts=tx.attempts + len(tasks),
        )
        await self.order_repo.mark_paid(tx.order_id)
        await self.tx_repo.remove_active(tx.tx_id)
        self._log("tx_terminal", tx_id=tx.tx_id, order_id=tx.order_id, tx_state=tx.state)
        return tx

    async def _abort_phase(self, tx: TxRecord) -> TxRecord:
        tx = await self._transition_state(tx, TxState.ABORTING, error=tx.error)

        # Abort all stock items + payment in parallel
        tasks = []
        for item_id, amount in tx.stock_prepared_items:
            async def _abort_stock(iid=item_id, a=amount):
                r = await self._call_with_retries(
                    operation_name="abort",
                    operation=lambda iid=iid, a=a: self.stock_client.abort_item(tx.tx_id, iid, a),
                    tx=tx, phase="abort", participant="stock", item_id=iid,
                )
                return ("stock", iid, r)
            tasks.append(_abort_stock())

        if tx.payment_prepared and not tx.payment_committed:
            async def _abort_payment():
                r = await self._call_with_retries(
                    operation_name="abort",
                    operation=lambda: self.payment_client.abort(tx.tx_id),
                    tx=tx, phase="abort", participant="payment",
                )
                return ("payment", None, r)
            tasks.append(_abort_payment())

        if tasks:
            results = await asyncio.gather(*tasks)
            for kind, iid, result in results:
                if not result.ok:
                    tx = await self.tx_repo.update(
                        tx.tx_id,
                        state=TxState.ABORTING.value,
                        error=result.detail or f"{kind} rollback failed",
                    )
                    return tx

        tx = await self._transition_state(tx, TxState.ABORTED, error=tx.error)
        await self.tx_repo.remove_active(tx.tx_id)
        await self.tx_repo.clear_order_tx(tx.order_id)
        self._log(
            "tx_terminal",
            tx_id=tx.tx_id,
            order_id=tx.order_id,
            tx_state=tx.state,
            detail=tx.error,
        )
        return tx

    async def _start_abort(self, tx_id: str, error_detail: str) -> TxRecord:
        tx = await self.tx_repo.get(tx_id)
        if tx is None:
            raise HTTPException(status_code=400, detail="Transaction not found")
        tx = await self._transition_state(tx, TxState.ABORTING, error=error_detail)
        return await self._abort_phase(tx)

    async def _call_with_retries(
        self,
        operation_name: str,
        operation: Callable[[], Awaitable[ParticipantResult]],
        tx: TxRecord,
        phase: str,
        participant: str,
        item_id: str | None = None,
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
                self._log(
                    "participant_failed",
                    level="warning",
                    tx_id=tx.tx_id,
                    phase=phase,
                    participant=participant,
                    item_id=item_id,
                    attempt=attempt,
                    detail=result.detail,
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

    async def _transition_state(self, tx: TxRecord, next_state: TxState, error: str | None = None) -> TxRecord:
        return await self.tx_repo.update(tx.tx_id, state=next_state.value, error=error)

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="order-service",
            component="two_pc",
            **fields,
        )

    @staticmethod
    def _aggregate_items(order_entry: OrderValue) -> list[tuple[str, int]]:
        totals: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            totals[item_id] += quantity
        return [(item_id, qty) for item_id, qty in totals.items()]
