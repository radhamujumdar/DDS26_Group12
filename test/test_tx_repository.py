import sys
import unittest
from pathlib import Path

from msgspec import msgpack

ORDER_DIR = Path(__file__).resolve().parents[1] / "order"
if str(ORDER_DIR) not in sys.path:
    sys.path.insert(0, str(ORDER_DIR))

from models import OrderValue, SagaState, TxRecord, TxState
from repository.saga_repo import SagaTxRepository
from repository.tx_repo import TX_FINALIZE_ABORT_SCRIPT, TX_FINALIZE_COMMIT_SCRIPT, TX_TRANSITION_SCRIPT, TxRepository


class FakePipeline:
    def __init__(self, db: "FakeRedis") -> None:
        self.db = db
        self.operations: list[tuple[str, str, object]] = []

    async def __aenter__(self) -> "FakePipeline":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def set(self, key: str, value: object) -> None:
        self.operations.append(("set", key, value))

    def sadd(self, key: str, value: object) -> None:
        self.operations.append(("sadd", key, value))

    async def execute(self) -> None:
        for operation, key, value in self.operations:
            if operation == "set":
                self.db.values[key] = value
            elif operation == "sadd":
                self.db.sets.setdefault(key, set()).add(value)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.sets: dict[str, set[object]] = {}

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        del transaction
        return FakePipeline(self)

    async def eval(self, script: str, numkeys: int, *args):
        del script, numkeys
        order_key, tx_key, active_key, tx_id, payload = args
        existing = self.values.get(order_key)
        if existing is not None:
            return [existing, 0]
        self.values[order_key] = tx_id
        self.values[tx_key] = payload
        self.sets.setdefault(active_key, set()).add(tx_id)
        return [tx_id, 1]

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: object, nx: bool = False, ex: int | None = None):
        del ex
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def delete(self, key: str) -> int:
        existed = key in self.values
        self.values.pop(key, None)
        return 1 if existed else 0

    async def sadd(self, key: str, value: object) -> int:
        self.sets.setdefault(key, set()).add(value)
        return 1

    async def srem(self, key: str, value: object) -> int:
        if key not in self.sets:
            return 0
        self.sets[key].discard(value)
        return 1

    async def smembers(self, key: str):
        return self.sets.get(key, set())

    def register_script(self, script: str):
        async def _runner(*, keys: list[str], args: list[object]):
            if script == TX_TRANSITION_SCRIPT:
                return self._run_tx_transition(keys, args)
            if script == TX_FINALIZE_COMMIT_SCRIPT:
                return self._run_finalize_commit(keys, args)
            if script == TX_FINALIZE_ABORT_SCRIPT:
                return self._run_finalize_abort(keys, args)
            raise AssertionError("Unexpected script registration")

        return _runner

    def _run_tx_transition(self, keys: list[str], args: list[object]):
        tx_key = keys[0]
        raw = self.values.get(tx_key)
        if raw is None:
            return [0, "Transaction not found"]

        tx = msgpack.decode(raw, type=TxRecord)
        tx.state = str(args[0])
        tx.updated_at = float(args[1])
        tx.attempts += int(args[2])
        tx.error = str(args[4]) if str(args[3]) == "1" else None
        if str(args[5]) == "1":
            tx.stock_prepared_items = [(item[0], int(item[1])) for item in msgpack.decode(args[6])]
        if str(args[7]) == "1":
            tx.stock_committed_items = [(item[0], int(item[1])) for item in msgpack.decode(args[8])]
        if str(args[9]) == "1":
            tx.payment_prepared = str(args[10]) == "1"
        if str(args[11]) == "1":
            tx.payment_committed = str(args[12]) == "1"

        packed = msgpack.encode(tx)
        self.values[tx_key] = packed
        return [1, packed]

    def _run_finalize_commit(self, keys: list[str], args: list[object]):
        tx_key, active_key = keys
        raw = self.values.get(tx_key)
        if raw is None:
            return [0, "Transaction not found"]

        tx = msgpack.decode(raw, type=TxRecord)
        order_raw = self.values.get(tx.order_id)
        if order_raw is None:
            return [-1, "Order not found"]

        order = msgpack.decode(order_raw, type=OrderValue)
        order.paid = True
        tx.state = str(args[0])
        tx.updated_at = float(args[1])
        tx.attempts += int(args[2])
        tx.stock_committed_items = [(item[0], int(item[1])) for item in msgpack.decode(args[3])]
        tx.payment_committed = str(args[4]) == "1"
        tx.error = None

        packed = msgpack.encode(tx)
        self.values[tx_key] = packed
        self.values[tx.order_id] = msgpack.encode(order)
        self.sets.setdefault(active_key, set()).discard(tx.tx_id)
        return [1, packed]

    def _run_finalize_abort(self, keys: list[str], args: list[object]):
        tx_key, active_key = keys
        raw = self.values.get(tx_key)
        if raw is None:
            return [0, "Transaction not found"]

        tx = msgpack.decode(raw, type=TxRecord)
        order_key = str(args[4]) + tx.order_id
        tx.state = str(args[0])
        tx.updated_at = float(args[1])
        tx.error = str(args[3]) if str(args[2]) == "1" else None

        packed = msgpack.encode(tx)
        self.values[tx_key] = packed
        self.sets.setdefault(active_key, set()).discard(tx.tx_id)
        if self.values.get(order_key) == tx.tx_id:
            self.values.pop(order_key, None)
        return [1, packed]


class TxRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_persist_prepare_success_updates_tx_atomically(self) -> None:
        db = FakeRedis()
        repo = TxRepository(db)
        tx = await repo.create(
            order_id="order-prepare-ok",
            user_id="user-prepare-ok",
            total_cost=20,
            items=[("item-1", 2)],
            state=TxState.INIT,
        )

        updated = await repo.persist_prepare_outcome(
            tx_id=tx.tx_id,
            stock_prepared_items=[("item-1", 2)],
            payment_prepared=True,
            attempts_increment=2,
        )

        self.assertEqual(updated.state, TxState.PREPARED.value)
        self.assertEqual(updated.stock_prepared_items, [("item-1", 2)])
        self.assertTrue(updated.payment_prepared)
        self.assertEqual(updated.attempts, 2)
        stored = await repo.get(tx.tx_id)
        self.assertIsNotNone(stored)
        self.assertEqual(stored.state, TxState.PREPARED.value)

    async def test_persist_prepare_failure_transitions_to_aborting(self) -> None:
        db = FakeRedis()
        repo = TxRepository(db)
        tx = await repo.create(
            order_id="order-prepare-fail",
            user_id="user-prepare-fail",
            total_cost=10,
            items=[("item-2", 1)],
            state=TxState.INIT,
        )

        updated = await repo.persist_prepare_outcome(
            tx_id=tx.tx_id,
            stock_prepared_items=[("item-2", 1)],
            payment_prepared=False,
            attempts_increment=1,
            error="Out of stock",
        )

        self.assertEqual(updated.state, TxState.ABORTING.value)
        self.assertEqual(updated.error, "Out of stock")
        self.assertEqual(updated.stock_prepared_items, [("item-2", 1)])
        self.assertEqual(updated.attempts, 1)

    async def test_persist_commit_progress_keeps_partial_commit_state(self) -> None:
        db = FakeRedis()
        repo = TxRepository(db)
        tx = await repo.create(
            order_id="order-commit-progress",
            user_id="user-commit-progress",
            total_cost=15,
            items=[("item-3", 1)],
            state=TxState.INIT,
        )
        await repo.persist_prepare_outcome(
            tx_id=tx.tx_id,
            stock_prepared_items=[("item-3", 1)],
            payment_prepared=True,
            attempts_increment=1,
        )

        updated = await repo.persist_commit_progress(
            tx_id=tx.tx_id,
            stock_committed_items=[("item-3", 1)],
            payment_committed=False,
            attempts_increment=1,
            error="Payment commit failed",
        )

        self.assertEqual(updated.state, TxState.COMMITTING.value)
        self.assertEqual(updated.stock_committed_items, [("item-3", 1)])
        self.assertFalse(updated.payment_committed)
        self.assertEqual(updated.error, "Payment commit failed")
        self.assertEqual(updated.attempts, 2)

    async def test_finalize_commit_marks_order_paid_and_cleans_active_index(self) -> None:
        db = FakeRedis()
        repo = TxRepository(db)
        db.values["order-commit-ok"] = msgpack.encode(
            OrderValue(paid=False, items=[("item-4", 1)], user_id="user-commit-ok", total_cost=25)
        )
        tx = await repo.create(
            order_id="order-commit-ok",
            user_id="user-commit-ok",
            total_cost=25,
            items=[("item-4", 1)],
            state=TxState.INIT,
        )
        await repo.persist_prepare_outcome(
            tx_id=tx.tx_id,
            stock_prepared_items=[("item-4", 1)],
            payment_prepared=True,
            attempts_increment=1,
        )

        updated = await repo.finalize_commit(
            tx_id=tx.tx_id,
            stock_committed_items=[("item-4", 1)],
            payment_committed=True,
            attempts_increment=2,
        )

        self.assertEqual(updated.state, TxState.COMMITTED.value)
        self.assertNotIn(tx.tx_id, db.sets[repo.TX_ACTIVE])
        order = msgpack.decode(db.values["order-commit-ok"], type=OrderValue)
        self.assertTrue(order.paid)

    async def test_finalize_abort_clears_order_mapping_and_active_index(self) -> None:
        db = FakeRedis()
        repo = TxRepository(db)
        tx = await repo.create(
            order_id="order-abort-ok",
            user_id="user-abort-ok",
            total_cost=12,
            items=[("item-5", 1)],
            state=TxState.INIT,
        )
        await repo.persist_prepare_outcome(
            tx_id=tx.tx_id,
            stock_prepared_items=[("item-5", 1)],
            payment_prepared=True,
            attempts_increment=1,
            error="prepare failed",
        )

        updated = await repo.finalize_abort(tx.tx_id, error="prepare failed")

        self.assertEqual(updated.state, TxState.ABORTED.value)
        self.assertEqual(updated.error, "prepare failed")
        self.assertNotIn(tx.tx_id, db.sets[repo.TX_ACTIVE])
        self.assertNotIn(repo._tx_order_key("order-abort-ok"), db.values)

    async def test_get_or_create_by_order_reuses_existing_2pc_transaction(self) -> None:
        repo = TxRepository(FakeRedis())

        created, created_flag = await repo.get_or_create_by_order(
            order_id="order-1",
            user_id="user-1",
            total_cost=2,
            items=[("item-1", 1), ("item-2", 1)],
            state=TxState.INIT,
        )
        existing, existing_flag = await repo.get_or_create_by_order(
            order_id="order-1",
            user_id="user-1",
            total_cost=2,
            items=[("item-1", 1), ("item-2", 1)],
            state=TxState.INIT,
        )

        self.assertTrue(created_flag)
        self.assertFalse(existing_flag)
        self.assertEqual(existing.tx_id, created.tx_id)

    async def test_get_or_create_by_order_rebuilds_after_stale_mapping(self) -> None:
        db = FakeRedis()
        repo = TxRepository(db)
        db.values[repo._tx_order_key("order-2")] = "missing-tx"

        created, created_flag = await repo.get_or_create_by_order(
            order_id="order-2",
            user_id="user-2",
            total_cost=5,
            items=[("item-3", 1)],
            state=TxState.INIT,
        )

        self.assertTrue(created_flag)
        self.assertEqual(db.values[repo._tx_order_key("order-2")], created.tx_id)

    async def test_get_or_create_by_order_reuses_existing_saga_transaction(self) -> None:
        repo = SagaTxRepository(FakeRedis())

        created, created_flag = await repo.get_or_create_by_order(
            order_id="order-3",
            user_id="user-3",
            total_cost=3,
            items=[("item-4", 1)],
            state=SagaState.INIT,
        )
        existing, existing_flag = await repo.get_or_create_by_order(
            order_id="order-3",
            user_id="user-3",
            total_cost=3,
            items=[("item-4", 1)],
            state=SagaState.INIT,
        )

        self.assertTrue(created_flag)
        self.assertFalse(existing_flag)
        self.assertEqual(existing.tx_id, created.tx_id)


if __name__ == "__main__":
    unittest.main()
