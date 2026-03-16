import importlib.util
import sys
import types
import unittest
from pathlib import Path

from msgspec import msgpack


ROOT = Path(__file__).resolve().parents[1]


class ModuleSandbox:
    def __init__(self) -> None:
        self._loaded: list[str] = []

    def add_package(self, name: str) -> None:
        package = types.ModuleType(name)
        package.__path__ = []  # type: ignore[attr-defined]
        sys.modules[name] = package
        self._loaded.append(name)

    def load(self, name: str, path: Path):
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load spec for {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        self._loaded.append(name)
        spec.loader.exec_module(module)
        if "." in name:
            parent_name, child_name = name.rsplit(".", 1)
            parent = sys.modules[parent_name]
            setattr(parent, child_name, module)
        return module

    def cleanup(self) -> None:
        for name in reversed(self._loaded):
            sys.modules.pop(name, None)


class FakeRedis:
    def __init__(self, repo_module, models_module) -> None:
        self.repo_module = repo_module
        self.models_module = models_module
        self.values: dict[str, bytes] = {}
        self.sets: dict[str, set[str]] = {}
        self.get_calls: list[str] = []

    def register_script(self, script: str):
        async def _runner(*, keys: list[str], args: list[object]):
            if script == self.repo_module.PAYMENT_PREPARE_LUA:
                return self._prepare(keys, args)
            if script == self.repo_module.PAYMENT_COMMIT_LUA:
                return self._commit(keys)
            if script == self.repo_module.PAYMENT_ABORT_LUA:
                return self._abort(keys)
            raise AssertionError("Unexpected payment script")

        return _runner

    async def get(self, key: str):
        self.get_calls.append(key)
        return self.values.get(key)

    async def set(self, key: str, value: bytes, nx: bool = False, ex: int | None = None):
        del ex
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def mset(self, kv_pairs: dict[str, bytes]) -> bool:
        self.values.update(kv_pairs)
        return True

    async def sadd(self, key: str, value: str) -> int:
        self.sets.setdefault(key, set()).add(value)
        return 1

    async def srem(self, key: str, value: str) -> int:
        self.sets.setdefault(key, set()).discard(value)
        return 1

    async def scard(self, key: str) -> int:
        return len(self.sets.get(key, set()))

    def _decode_user(self, raw: bytes):
        return msgpack.decode(raw, type=self.models_module.UserValue)

    def _decode_prepare(self, raw: bytes):
        return msgpack.decode(raw, type=self.models_module.PrepareRecord)

    def _prepare(self, keys: list[str], args: list[object]):
        user_key, tx_key, prep_key = keys
        txn_id, user_id, amount = str(args[0]), str(args[1]), int(args[2])
        existing_raw = self.values.get(tx_key)
        if existing_raw is not None:
            existing = self._decode_prepare(existing_raw)
            if existing.user_id != user_id or existing.delta != -amount:
                return [-1, "Transaction parameters mismatch"]
            if existing.state == self.models_module.TxnState.ABORTED:
                return [-2, f"Insufficient credit for user {user_id}"]
            return [1, existing_raw]

        user_raw = self.values.get(user_key)
        if user_raw is None:
            return [-1, "User not found"]

        user = self._decode_user(user_raw)
        if user.credit < amount:
            record = self.models_module.PrepareRecord(
                txn_id=txn_id,
                user_id=user_id,
                delta=-amount,
                old_credit=user.credit,
                new_credit=user.credit,
                state=self.models_module.TxnState.ABORTED,
            )
            self.values[tx_key] = msgpack.encode(record)
            return [-2, f"Insufficient credit for user {user_id}"]

        record = self.models_module.PrepareRecord(
            txn_id=txn_id,
            user_id=user_id,
            delta=-amount,
            old_credit=user.credit,
            new_credit=user.credit - amount,
            state=self.models_module.TxnState.PREPARED,
        )
        self.values[user_key] = msgpack.encode(self.models_module.UserValue(credit=user.credit - amount))
        self.values[tx_key] = msgpack.encode(record)
        self.sets.setdefault(prep_key, set()).add(txn_id)
        return [0, self.values[tx_key]]

    def _commit(self, keys: list[str]):
        tx_key = keys[0]
        raw = self.values.get(tx_key)
        if raw is None:
            return [-1, "Unknown transaction"]

        record = self._decode_prepare(raw)
        prep_key = self.repo_module.prepared_user_key(record.user_id)
        if record.state == self.models_module.TxnState.ABORTED:
            return [-1, "Transaction was already aborted"]
        if record.state == self.models_module.TxnState.COMMITTED:
            self.sets.setdefault(prep_key, set()).discard(record.txn_id)
            return [0, raw]
        if record.state != self.models_module.TxnState.PREPARED:
            return [-1, "Transaction in invalid state"]

        committed = self.models_module.PrepareRecord(
            txn_id=record.txn_id,
            user_id=record.user_id,
            delta=record.delta,
            old_credit=record.old_credit,
            new_credit=record.new_credit,
            state=self.models_module.TxnState.COMMITTED,
        )
        self.values[tx_key] = msgpack.encode(committed)
        self.sets.setdefault(prep_key, set()).discard(record.txn_id)
        return [0, self.values[tx_key]]

    def _abort(self, keys: list[str]):
        tx_key = keys[0]
        raw = self.values.get(tx_key)
        if raw is None:
            return [0, "aborted"]

        record = self._decode_prepare(raw)
        prep_key = self.repo_module.prepared_user_key(record.user_id)
        if record.state == self.models_module.TxnState.COMMITTED:
            return [-1, "Cannot abort committed transaction"]
        if record.state == self.models_module.TxnState.ABORTED:
            self.sets.setdefault(prep_key, set()).discard(record.txn_id)
            return [0, raw]
        if record.state != self.models_module.TxnState.PREPARED:
            return [-1, "Transaction in invalid state"]

        user_raw = self.values.get(record.user_id)
        if user_raw is not None:
            user = self._decode_user(user_raw)
            self.values[record.user_id] = msgpack.encode(
                self.models_module.UserValue(credit=user.credit - record.delta)
            )

        aborted = self.models_module.PrepareRecord(
            txn_id=record.txn_id,
            user_id=record.user_id,
            delta=record.delta,
            old_credit=record.old_credit,
            new_credit=record.new_credit,
            state=self.models_module.TxnState.ABORTED,
        )
        self.values[tx_key] = msgpack.encode(aborted)
        self.sets.setdefault(prep_key, set()).discard(record.txn_id)
        return [0, self.values[tx_key]]


class PaymentRepositoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.sandbox = ModuleSandbox()
        self.sandbox.add_package("repository")
        self.models_module = self.sandbox.load("models", ROOT / "payment/models.py")
        self.repo_module = self.sandbox.load(
            "repository.payment_repo",
            ROOT / "payment/repository/payment_repo.py",
        )

    def tearDown(self) -> None:
        self.sandbox.cleanup()

    async def test_prepare_transaction_returns_record_without_trailing_get(self) -> None:
        db = FakeRedis(self.repo_module, self.models_module)
        repo = self.repo_module.PaymentRepository(db)
        db.values["user-1"] = msgpack.encode(self.models_module.UserValue(credit=100))

        record = await repo.prepare_transaction("txn-1", "user-1", 30)

        self.assertEqual(record.txn_id, "txn-1")
        self.assertEqual(record.user_id, "user-1")
        self.assertEqual(record.delta, -30)
        self.assertEqual(record.old_credit, 100)
        self.assertEqual(record.new_credit, 70)
        self.assertEqual(record.state, self.models_module.TxnState.PREPARED)
        self.assertEqual(db.get_calls, [])

    async def test_commit_transaction_uses_script_payload_without_metadata_preread(self) -> None:
        db = FakeRedis(self.repo_module, self.models_module)
        repo = self.repo_module.PaymentRepository(db)
        prepared = self.models_module.PrepareRecord(
            txn_id="txn-2",
            user_id="user-2",
            delta=-25,
            old_credit=120,
            new_credit=95,
            state=self.models_module.TxnState.PREPARED,
        )
        db.values[self.repo_module.txn_key("txn-2")] = msgpack.encode(prepared)
        db.sets[self.repo_module.prepared_user_key("user-2")] = {"txn-2"}

        record = await repo.commit_transaction("txn-2")

        self.assertEqual(record.state, self.models_module.TxnState.COMMITTED)
        self.assertEqual(record.user_id, "user-2")
        self.assertEqual(db.get_calls, [])
        self.assertEqual(db.sets[self.repo_module.prepared_user_key("user-2")], set())

    async def test_abort_transaction_uses_script_derived_metadata_without_preread(self) -> None:
        db = FakeRedis(self.repo_module, self.models_module)
        repo = self.repo_module.PaymentRepository(db)
        db.values["user-3"] = msgpack.encode(self.models_module.UserValue(credit=40))
        prepared = self.models_module.PrepareRecord(
            txn_id="txn-3",
            user_id="user-3",
            delta=-15,
            old_credit=55,
            new_credit=40,
            state=self.models_module.TxnState.PREPARED,
        )
        db.values[self.repo_module.txn_key("txn-3")] = msgpack.encode(prepared)
        db.sets[self.repo_module.prepared_user_key("user-3")] = {"txn-3"}

        aborted = await repo.abort_transaction("txn-3")

        self.assertTrue(aborted)
        self.assertEqual(db.get_calls, [])
        refunded_user = msgpack.decode(db.values["user-3"], type=self.models_module.UserValue)
        self.assertEqual(refunded_user.credit, 55)
        stored = msgpack.decode(
            db.values[self.repo_module.txn_key("txn-3")],
            type=self.models_module.PrepareRecord,
        )
        self.assertEqual(stored.state, self.models_module.TxnState.ABORTED)


if __name__ == "__main__":
    unittest.main()
