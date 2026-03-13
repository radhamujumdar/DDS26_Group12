import sys
import unittest
from pathlib import Path


ORDER_DIR = Path(__file__).resolve().parents[1] / "order"
if str(ORDER_DIR) not in sys.path:
    sys.path.insert(0, str(ORDER_DIR))

from models import SagaState, TxState
from repository.saga_repo import SagaTxRepository
from repository.tx_repo import TxRepository


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


class TxRepositoryTests(unittest.IsolatedAsyncioTestCase):
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
