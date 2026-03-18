import asyncio
import logging
import time
import unittest
import uuid
from pathlib import Path
import sys

from fastapi import HTTPException


ORDER_DIR = Path(__file__).resolve().parents[1] / "order"
if str(ORDER_DIR) not in sys.path:
    sys.path.insert(0, str(ORDER_DIR))

from coordinator.saga import SagaCoordinator
from models import OrderValue, ParticipantResult, SagaState, SagaTxRecord


def _now() -> float:
    return time.time()


class FakeOrderRepository:
    def __init__(self):
        self.orders: dict[str, OrderValue] = {}

    async def mark_paid(self, order_id: str):
        entry = self.orders[order_id]
        self.orders[order_id] = OrderValue(
            paid=True,
            items=list(entry.items),
            user_id=entry.user_id,
            total_cost=entry.total_cost,
        )


class FakeSagaTxRepository:
    def __init__(self):
        self.records: dict[str, SagaTxRecord] = {}
        self.order_to_tx: dict[str, str] = {}
        self.active: set[str] = set()
        self.tx_locks: dict[str, str] = {}
        self.create_count = 0

    async def get_or_create_by_order(
        self,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: SagaState = SagaState.INIT,
        wait_timeout_seconds: float = 2.0,
    ) -> tuple[SagaTxRecord, bool]:
        del wait_timeout_seconds
        tx_id = self.order_to_tx.get(order_id)
        if tx_id:
            return self.records[tx_id], False

        self.create_count += 1
        tx = SagaTxRecord(
            tx_id=f"saga-{self.create_count}-{uuid.uuid4()}",
            order_id=order_id,
            user_id=user_id,
            total_cost=total_cost,
            items=list(items),
            state=state.value,
            created_at=_now(),
            updated_at=_now(),
        )
        self.records[tx.tx_id] = tx
        self.order_to_tx[order_id] = tx.tx_id
        self.active.add(tx.tx_id)
        return tx, True

    async def get(self, tx_id: str) -> SagaTxRecord | None:
        return self.records.get(tx_id)

    async def update(self, tx_id: str, **changes) -> SagaTxRecord:
        existing = self.records.get(tx_id)
        if existing is None:
            raise HTTPException(status_code=400, detail=f"Saga transaction {tx_id} not found")
        updated = SagaTxRecord(
            tx_id=existing.tx_id,
            order_id=existing.order_id,
            user_id=changes.get("user_id", existing.user_id),
            total_cost=changes.get("total_cost", existing.total_cost),
            items=changes.get("items", existing.items),
            state=changes.get("state", existing.state),
            created_at=existing.created_at,
            updated_at=_now(),
            stock_reserved_items=changes.get("stock_reserved_items", existing.stock_reserved_items),
            stock_released_items=changes.get("stock_released_items", existing.stock_released_items),
            payment_debited=changes.get("payment_debited", existing.payment_debited),
            payment_refunded=changes.get("payment_refunded", existing.payment_refunded),
            attempts=changes.get("attempts", existing.attempts),
            error=changes.get("error", existing.error),
        )
        self.records[tx_id] = updated
        return updated

    async def list_active(self) -> list[str]:
        return list(self.active)

    async def add_active(self, tx_id: str):
        self.active.add(tx_id)

    async def remove_active(self, tx_id: str):
        self.active.discard(tx_id)

    async def clear_order_tx(self, order_id: str):
        self.order_to_tx.pop(order_id, None)

    async def acquire_tx_lock(self, tx_id: str, ttl_seconds: int = 30) -> str | None:
        del ttl_seconds
        if tx_id in self.tx_locks:
            return None
        token = f"token-{uuid.uuid4()}"
        self.tx_locks[tx_id] = token
        return token

    async def renew_tx_lock(self, tx_id: str, token: str, ttl_seconds: int = 30) -> bool:
        del ttl_seconds
        return self.tx_locks.get(tx_id) == token

    async def release_tx_lock(self, tx_id: str, token: str) -> bool:
        if self.tx_locks.get(tx_id) != token:
            return False
        del self.tx_locks[tx_id]
        return True


class FakeStockClient:
    def __init__(self):
        self.reserve_sequences: dict[str, list[ParticipantResult]] = {}
        self.release_sequences: dict[str, list[ParticipantResult]] = {}
        self.reserve_calls: list[tuple[str, str, int, int]] = []
        self.release_calls: list[tuple[str, str, int, int]] = []
        self.saga_bus = None

    def set_reserve_sequence(self, item_id: str, sequence: list[ParticipantResult]):
        self.reserve_sequences[item_id] = list(sequence)

    async def saga_reserve_item(self, tx_id: str, item_id: str, amount: int, attempt: int) -> ParticipantResult:
        self.reserve_calls.append((tx_id, item_id, amount, attempt))
        return self._pop_or_ok(self.reserve_sequences, item_id)

    async def saga_release_item(self, tx_id: str, item_id: str, amount: int, attempt: int) -> ParticipantResult:
        self.release_calls.append((tx_id, item_id, amount, attempt))
        return self._pop_or_ok(self.release_sequences, item_id)

    @staticmethod
    def _pop_or_ok(store: dict[str, list[ParticipantResult]], key: str) -> ParticipantResult:
        sequence = store.get(key)
        if sequence:
            return sequence.pop(0)
        return ParticipantResult(ok=True)


class FakePaymentClient:
    def __init__(self):
        self.debit_sequence: list[ParticipantResult] = []
        self.refund_sequence: list[ParticipantResult] = []
        self.debit_calls: list[tuple[str, str, int, int]] = []
        self.refund_calls: list[tuple[str, str, int, int]] = []
        self.saga_bus = None

    async def saga_debit(self, tx_id: str, user_id: str, amount: int, attempt: int) -> ParticipantResult:
        self.debit_calls.append((tx_id, user_id, amount, attempt))
        if self.debit_sequence:
            return self.debit_sequence.pop(0)
        return ParticipantResult(ok=True)

    async def saga_refund(self, tx_id: str, user_id: str, amount: int, attempt: int) -> ParticipantResult:
        self.refund_calls.append((tx_id, user_id, amount, attempt))
        if self.refund_sequence:
            return self.refund_sequence.pop(0)
        return ParticipantResult(ok=True)


class FakeSagaBus:
    def __init__(self):
        self.response_timeout_ms = 3000
        self.late_results: dict[str, ParticipantResult] = {}
        self.await_calls: list[tuple[str, int]] = []

    def set_late_result(self, correlation_id: str, result: ParticipantResult):
        self.late_results[correlation_id] = result

    async def await_late_result(self, correlation_id: str, timeout_ms: int) -> ParticipantResult:
        self.await_calls.append((correlation_id, timeout_ms))
        return self.late_results.get(
            correlation_id,
            ParticipantResult(
                ok=False,
                retryable=True,
                detail="Saga MQ response timeout",
                correlation_id=correlation_id,
                status="timed_out",
            ),
        )


class TestSagaCoordinator(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logger = logging.getLogger("saga-test")
        logger.handlers = []
        logger.addHandler(logging.NullHandler())

        self.order_repo = FakeOrderRepository()
        self.saga_repo = FakeSagaTxRepository()
        self.stock_client = FakeStockClient()
        self.payment_client = FakePaymentClient()
        self.stock_bus = FakeSagaBus()
        self.payment_bus = FakeSagaBus()
        self.stock_client.saga_bus = self.stock_bus
        self.payment_client.saga_bus = self.payment_bus
        self.coordinator = SagaCoordinator(
            stock_client=self.stock_client,
            payment_client=self.payment_client,
            saga_repo=self.saga_repo,
            order_repo=self.order_repo,
            logger=logger,
        )
        self.coordinator.RETRY_LIMIT = 1
        self.coordinator.RETRY_BACKOFF_SECONDS = 0.0
        self.coordinator.IN_FLIGHT_WAIT_SECONDS = 0.2
        self.coordinator.IN_FLIGHT_POLL_SECONDS = 0.01
        self.coordinator.TX_LOCK_RENEW_INTERVAL_SECONDS = 60.0

    async def test_uncertain_stock_reserve_still_triggers_release(self):
        order_id = "order-stock-timeout"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1)],
            user_id="user-1",
            total_cost=10,
        )
        self.order_repo.orders[order_id] = order
        self.stock_client.set_reserve_sequence(
            "item-1",
            [ParticipantResult(ok=False, retryable=True, detail="Saga MQ response timeout")],
        )

        with self.assertRaises(HTTPException):
            await self.coordinator.checkout(order_id, order)

        tx = next(iter(self.saga_repo.records.values()))
        self.assertEqual(tx.state, SagaState.COMPENSATED.value)
        self.assertFalse(self.order_repo.orders[order_id].paid)
        self.assertEqual(self.payment_client.debit_calls, [])
        self.assertEqual(len(self.stock_client.release_calls), 1)
        self.assertEqual(self.stock_client.release_calls[0][1:3], ("item-1", 1))
        self.assertNotIn(order_id, self.saga_repo.order_to_tx)

    async def test_uncertain_payment_debit_triggers_refund(self):
        order_id = "order-payment-timeout"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1)],
            user_id="user-2",
            total_cost=25,
        )
        self.order_repo.orders[order_id] = order
        self.payment_client.debit_sequence = [
            ParticipantResult(ok=False, retryable=True, detail="Saga MQ response timeout"),
        ]

        with self.assertRaises(HTTPException):
            await self.coordinator.checkout(order_id, order)

        tx = next(iter(self.saga_repo.records.values()))
        self.assertEqual(tx.state, SagaState.COMPENSATED.value)
        self.assertFalse(self.order_repo.orders[order_id].paid)
        self.assertEqual(len(self.payment_client.refund_calls), 1)
        self.assertEqual(self.payment_client.refund_calls[0][0], tx.tx_id)
        self.assertEqual(self.payment_client.refund_calls[0][1:3], (order.user_id, order.total_cost))
        self.assertEqual(len(self.stock_client.release_calls), 1)
        self.assertEqual(self.stock_client.release_calls[0][1:3], ("item-1", 1))

    async def test_late_successful_stock_release_finalizes_compensation(self):
        order_id = "order-late-release"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1)],
            user_id="user-3",
            total_cost=10,
        )
        self.order_repo.orders[order_id] = order
        release_timeout = ParticipantResult(
            ok=False,
            retryable=True,
            detail="Saga MQ response timeout",
            correlation_id="corr-release-1",
            status="timed_out",
        )
        self.payment_client.debit_sequence = [
            ParticipantResult(ok=False, retryable=False, detail="Insufficient credit"),
        ]
        self.stock_client.release_sequences["item-1"] = [release_timeout]
        self.stock_bus.set_late_result(
            "corr-release-1",
            ParticipantResult(
                ok=True,
                retryable=False,
                detail="released",
                correlation_id="corr-release-1",
                status="completed",
            ),
        )

        with self.assertRaises(HTTPException):
            await self.coordinator.checkout(order_id, order)

        tx = next(iter(self.saga_repo.records.values()))
        self.assertEqual(tx.state, SagaState.COMPENSATED.value)
        self.assertEqual(tx.stock_released_items, [("item-1", 1)])
        self.assertNotIn(tx.tx_id, self.saga_repo.active)
        self.assertNotIn(order_id, self.saga_repo.order_to_tx)
        self.assertEqual(self.stock_bus.await_calls, [("corr-release-1", 3000)])

    async def test_late_successful_payment_refund_allows_compensation_to_finish(self):
        order_id = "order-late-refund"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1)],
            user_id="user-4",
            total_cost=10,
        )
        self.order_repo.orders[order_id] = order
        tx, _ = await self.saga_repo.get_or_create_by_order(
            order_id=order_id,
            user_id=order.user_id,
            total_cost=order.total_cost,
            items=list(order.items),
            state=SagaState.COMPENSATING,
        )
        await self.saga_repo.update(
            tx.tx_id,
            state=SagaState.COMPENSATING.value,
            payment_debited=True,
            stock_reserved_items=[("item-1", 1)],
            error="Saga MQ response timeout",
        )
        refund_timeout = ParticipantResult(
            ok=False,
            retryable=True,
            detail="Saga MQ response timeout",
            correlation_id="corr-refund-1",
            status="timed_out",
        )
        self.payment_client.refund_sequence = [refund_timeout]
        self.payment_bus.set_late_result(
            "corr-refund-1",
            ParticipantResult(
                ok=True,
                retryable=False,
                detail="refunded",
                correlation_id="corr-refund-1",
                status="completed",
            ),
        )

        tx = await self.coordinator._process_transaction(tx.tx_id)

        self.assertEqual(tx.state, SagaState.COMPENSATED.value)
        self.assertTrue(tx.payment_refunded)
        self.assertEqual(tx.stock_released_items, [("item-1", 1)])
        self.assertNotIn(tx.tx_id, self.saga_repo.active)
        self.assertNotIn(order_id, self.saga_repo.order_to_tx)
        self.assertEqual(self.payment_bus.await_calls, [("corr-refund-1", 3000)])


if __name__ == "__main__":
    unittest.main()
