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

from coordinator.two_pc import TwoPCCoordinator
from models import OrderValue, ParticipantResult, TxRecord, TxState


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


class FakeTxRepository:
    def __init__(self):
        self.records: dict[str, TxRecord] = {}
        self.order_to_tx: dict[str, str] = {}
        self.active: set[str] = set()
        self.tx_locks: set[str] = set()
        self.order_create_locks: dict[str, asyncio.Lock] = {}
        self.create_count = 0
        self.create_delay_seconds = 0.0

    async def get_or_create_by_order(
        self,
        order_id: str,
        user_id: str,
        total_cost: int,
        items: list[tuple[str, int]],
        state: TxState = TxState.INIT,
    ) -> tuple[TxRecord, bool]:
        tx_id = self.order_to_tx.get(order_id)
        if tx_id:
            return self.records[tx_id], False

        lock = self.order_create_locks.setdefault(order_id, asyncio.Lock())
        async with lock:
            tx_id = self.order_to_tx.get(order_id)
            if tx_id:
                return self.records[tx_id], False
            if self.create_delay_seconds:
                await asyncio.sleep(self.create_delay_seconds)
            self.create_count += 1
            tx = TxRecord(
                tx_id=f"tx-{self.create_count}-{uuid.uuid4()}",
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

    async def list_active(self) -> list[str]:
        return list(self.active)

    async def acquire_tx_lock(self, tx_id: str, ttl_seconds: int = 30) -> bool:
        del ttl_seconds
        if tx_id in self.tx_locks:
            return False
        self.tx_locks.add(tx_id)
        return True

    async def release_tx_lock(self, tx_id: str):
        self.tx_locks.discard(tx_id)

    async def get(self, tx_id: str) -> TxRecord | None:
        return self.records.get(tx_id)

    async def update(self, tx_id: str, **changes) -> TxRecord:
        existing = self.records.get(tx_id)
        if existing is None:
            raise HTTPException(status_code=400, detail=f"Transaction {tx_id} not found")
        updated = TxRecord(
            tx_id=existing.tx_id,
            order_id=existing.order_id,
            user_id=changes.get("user_id", existing.user_id),
            total_cost=changes.get("total_cost", existing.total_cost),
            items=changes.get("items", existing.items),
            state=changes.get("state", existing.state),
            created_at=existing.created_at,
            updated_at=_now(),
            stock_prepared_items=changes.get("stock_prepared_items", existing.stock_prepared_items),
            stock_committed_items=changes.get("stock_committed_items", existing.stock_committed_items),
            payment_prepared=changes.get("payment_prepared", existing.payment_prepared),
            payment_committed=changes.get("payment_committed", existing.payment_committed),
            attempts=changes.get("attempts", existing.attempts),
            error=changes.get("error", existing.error),
        )
        self.records[tx_id] = updated
        return updated

    async def remove_active(self, tx_id: str):
        self.active.discard(tx_id)

    async def clear_order_tx(self, order_id: str):
        self.order_to_tx.pop(order_id, None)

    def insert_record(self, record: TxRecord, active: bool = True, map_order: bool = True):
        self.records[record.tx_id] = record
        if active:
            self.active.add(record.tx_id)
        if map_order:
            self.order_to_tx[record.order_id] = record.tx_id


class FakeStockClient:
    def __init__(self):
        self.prepare_sequences: dict[str, list[ParticipantResult]] = {}
        self.commit_sequences: dict[str, list[ParticipantResult]] = {}
        self.abort_sequences: dict[str, list[ParticipantResult]] = {}
        self.prepare_calls: list[tuple[str, str, int]] = []
        self.commit_calls: list[tuple[str, str, int]] = []
        self.abort_calls: list[tuple[str, str, int]] = []
        self.prepare_delay_seconds = 0.0

    def set_prepare_sequence(self, item_id: str, sequence: list[ParticipantResult]):
        self.prepare_sequences[item_id] = list(sequence)

    def set_commit_sequence(self, item_id: str, sequence: list[ParticipantResult]):
        self.commit_sequences[item_id] = list(sequence)

    def set_abort_sequence(self, item_id: str, sequence: list[ParticipantResult]):
        self.abort_sequences[item_id] = list(sequence)

    async def prepare_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        self.prepare_calls.append((tx_id, item_id, amount))
        if self.prepare_delay_seconds:
            await asyncio.sleep(self.prepare_delay_seconds)
        return self._pop_or_ok(self.prepare_sequences, item_id)

    async def commit_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        self.commit_calls.append((tx_id, item_id, amount))
        return self._pop_or_ok(self.commit_sequences, item_id)

    async def abort_item(self, tx_id: str, item_id: str, amount: int) -> ParticipantResult:
        self.abort_calls.append((tx_id, item_id, amount))
        return self._pop_or_ok(self.abort_sequences, item_id)

    @staticmethod
    def _pop_or_ok(store: dict[str, list[ParticipantResult]], key: str) -> ParticipantResult:
        sequence = store.get(key)
        if sequence:
            return sequence.pop(0)
        return ParticipantResult(ok=True)


class FakePaymentClient:
    def __init__(self):
        self.prepare_sequence: list[ParticipantResult] = []
        self.commit_sequence: list[ParticipantResult] = []
        self.abort_sequence: list[ParticipantResult] = []
        self.prepare_calls: list[tuple[str, str, int]] = []
        self.commit_calls: list[str] = []
        self.abort_calls: list[str] = []

    async def prepare(self, tx_id: str, user_id: str, amount: int) -> ParticipantResult:
        self.prepare_calls.append((tx_id, user_id, amount))
        if self.prepare_sequence:
            return self.prepare_sequence.pop(0)
        return ParticipantResult(ok=True)

    async def commit(self, tx_id: str) -> ParticipantResult:
        self.commit_calls.append(tx_id)
        if self.commit_sequence:
            return self.commit_sequence.pop(0)
        return ParticipantResult(ok=True)

    async def abort(self, tx_id: str) -> ParticipantResult:
        self.abort_calls.append(tx_id)
        if self.abort_sequence:
            return self.abort_sequence.pop(0)
        return ParticipantResult(ok=True)


class TestTwoPCCoordinator(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.logger = logging.getLogger("two-pc-test")
        self.logger.handlers = []
        self.logger.addHandler(logging.NullHandler())

        self.order_repo = FakeOrderRepository()
        self.tx_repo = FakeTxRepository()
        self.stock_client = FakeStockClient()
        self.payment_client = FakePaymentClient()
        self.coordinator = TwoPCCoordinator(
            stock_client=self.stock_client,
            payment_client=self.payment_client,
            tx_repo=self.tx_repo,
            order_repo=self.order_repo,
            logger=self.logger,
        )
        self.coordinator.RETRY_BACKOFF_SECONDS = 0.0
        self.coordinator.IN_FLIGHT_WAIT_SECONDS = 0.2
        self.coordinator.IN_FLIGHT_POLL_SECONDS = 0.01

    async def test_checkout_success_commits_and_marks_order_paid(self):
        order_id = "order-1"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1), ("item-1", 2)],
            user_id="user-1",
            total_cost=30,
        )
        self.order_repo.orders[order_id] = order

        await self.coordinator.checkout(order_id, order)

        self.assertEqual(self.tx_repo.create_count, 1)
        tx = next(iter(self.tx_repo.records.values()))
        self.assertEqual(tx.state, TxState.COMMITTED.value)
        self.assertTrue(self.order_repo.orders[order_id].paid)

        self.assertEqual(len(self.stock_client.prepare_calls), 1)
        self.assertEqual(len(self.stock_client.commit_calls), 1)
        self.assertEqual(self.stock_client.prepare_calls[0][2], 3)
        self.assertEqual(self.stock_client.commit_calls[0][2], 3)
        self.assertEqual(len(self.payment_client.prepare_calls), 1)
        self.assertEqual(len(self.payment_client.commit_calls), 1)

    async def test_prepare_failure_aborts_without_marking_order_paid(self):
        order_id = "order-2"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1)],
            user_id="user-2",
            total_cost=10,
        )
        self.order_repo.orders[order_id] = order
        self.stock_client.set_prepare_sequence(
            "item-1",
            [ParticipantResult(ok=False, retryable=False, detail="Out of stock")],
        )

        with self.assertRaises(HTTPException):
            await self.coordinator.checkout(order_id, order)

        tx = next(iter(self.tx_repo.records.values()))
        self.assertEqual(tx.state, TxState.ABORTED.value)
        self.assertFalse(self.order_repo.orders[order_id].paid)
        self.assertEqual(len(self.payment_client.prepare_calls), 0)
        self.assertNotIn(order_id, self.tx_repo.order_to_tx)

    async def test_retryable_prepare_failure_retries_and_succeeds(self):
        order_id = "order-3"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1)],
            user_id="user-3",
            total_cost=10,
        )
        self.order_repo.orders[order_id] = order
        self.stock_client.set_prepare_sequence(
            "item-1",
            [
                ParticipantResult(ok=False, retryable=True, detail="temporary stock error"),
                ParticipantResult(ok=True),
            ],
        )

        await self.coordinator.checkout(order_id, order)

        tx = next(iter(self.tx_repo.records.values()))
        self.assertEqual(tx.state, TxState.COMMITTED.value)
        self.assertEqual(len(self.stock_client.prepare_calls), 2)

    async def test_duplicate_checkout_is_idempotent_and_creates_one_tx(self):
        order_id = "order-4"
        order = OrderValue(
            paid=False,
            items=[("item-1", 1)],
            user_id="user-4",
            total_cost=10,
        )
        self.order_repo.orders[order_id] = order
        self.tx_repo.create_delay_seconds = 0.02
        self.stock_client.prepare_delay_seconds = 0.05

        await asyncio.gather(
            self.coordinator.checkout(order_id, order),
            self.coordinator.checkout(order_id, order),
        )

        self.assertEqual(self.tx_repo.create_count, 1)
        self.assertEqual(len(self.tx_repo.records), 1)
        self.assertEqual(len(self.payment_client.commit_calls), 1)
        tx = next(iter(self.tx_repo.records.values()))
        self.assertEqual(tx.state, TxState.COMMITTED.value)

    async def test_recovery_processes_prepared_transaction_to_commit(self):
        order_id = "order-5"
        self.order_repo.orders[order_id] = OrderValue(
            paid=False,
            items=[("item-1", 2)],
            user_id="user-5",
            total_cost=20,
        )
        tx = TxRecord(
            tx_id="tx-recover-1",
            order_id=order_id,
            user_id="user-5",
            total_cost=20,
            items=[("item-1", 2)],
            state=TxState.PREPARED.value,
            created_at=_now(),
            updated_at=_now(),
            stock_prepared_items=[("item-1", 2)],
            payment_prepared=True,
        )
        self.tx_repo.insert_record(tx, active=True, map_order=True)

        await self.coordinator.recover_active_transactions()

        recovered = self.tx_repo.records["tx-recover-1"]
        self.assertEqual(recovered.state, TxState.COMMITTED.value)
        self.assertTrue(self.order_repo.orders[order_id].paid)
        self.assertNotIn("tx-recover-1", self.tx_repo.active)


if __name__ == "__main__":
    unittest.main()
