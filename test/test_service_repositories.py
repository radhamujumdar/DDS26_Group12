from __future__ import annotations

import asyncio
import unittest

import fluxi_sdk_test_support  # noqa: F401
from msgspec import msgpack
from redis.asyncio import Redis

from fluxi_engine_test_support import RedisHarness
from order.app.domain.models import OrderValue
from order.app.repositories.order_repository import OrderRepository
from payment.app.domain.errors import InsufficientCreditError
from payment.app.domain.models import UserValue
from payment.app.repositories.payment_repository import PaymentRepository
from shop_common.checkout import CheckoutItem, StockReservation
from stock.app.domain.errors import StockInsufficientError
from stock.app.domain.models import StockValue
from stock.app.repositories.stock_repository import StockRepository


class RepositoryRedisAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.harness = RedisHarness()
        cls.harness.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.harness.stop()
        super().tearDownClass()

    async def asyncSetUp(self) -> None:
        if self.harness.url is None:
            raise unittest.SkipTest("No Redis URL available for repository tests.")
        self.redis = Redis.from_url(self.harness.url, decode_responses=False)
        await self.redis.flushdb()

    async def asyncTearDown(self) -> None:
        await self.redis.flushdb()
        try:
            await self.redis.aclose(close_connection_pool=True)
        except TypeError:
            await self.redis.aclose()

    def create_redis_client(self) -> Redis:
        if self.harness.url is None:
            raise unittest.SkipTest("No Redis URL available for repository tests.")
        return Redis.from_url(self.harness.url, decode_responses=False)

    async def close_redis_clients(self, *clients: Redis) -> None:
        for client in clients:
            try:
                await client.aclose(close_connection_pool=True)
            except TypeError:
                await client.aclose()


class TestServiceRepositories(RepositoryRedisAsyncTestCase):
    async def test_order_add_item_accumulates_concurrent_updates(self) -> None:
        repository = OrderRepository(self.redis)
        await self.redis.set(
            "order-concurrent",
            msgpack.encode(
                OrderValue(
                    paid=False,
                    items=[],
                    user_id="user-1",
                    total_cost=0,
                )
            ),
        )
        clients = [self.create_redis_client() for _ in range(12)]
        repositories = [OrderRepository(client) for client in clients]

        try:
            await asyncio.gather(
                *(
                    repo.add_item(
                        "order-concurrent",
                        item_id=f"item-{index}",
                        quantity=1,
                        item_price=10,
                    )
                    for index, repo in enumerate(repositories)
                )
            )
        finally:
            await self.close_redis_clients(*clients)

        order = await repository.get_order("order-concurrent")
        self.assertEqual(order.total_cost, 120)
        self.assertCountEqual(
            order.items,
            [(f"item-{index}", 1) for index in range(12)],
        )

    async def test_stock_reserve_is_idempotent_across_duplicate_delivery(self) -> None:
        repository = StockRepository(self.redis)
        await self.redis.set("item-1", msgpack.encode(StockValue(stock=5, price=10)))

        first = await repository.reserve_stock_idempotent(
            "item-1",
            quantity=2,
            activity_execution_id="act-stock-1",
        )
        second = await repository.reserve_stock_idempotent(
            "item-1",
            quantity=2,
            activity_execution_id="act-stock-1",
        )

        self.assertEqual(first, second)
        item = await repository.get_item("item-1")
        self.assertEqual(item.stock, 3)

    async def test_stock_batch_reserve_and_release_are_idempotent(self) -> None:
        repository = StockRepository(self.redis)
        await self.redis.mset(
            {
                "item-a": msgpack.encode(StockValue(stock=5, price=10)),
                "item-b": msgpack.encode(StockValue(stock=7, price=12)),
            }
        )
        items = (
            CheckoutItem(item_id="item-a", quantity=2),
            CheckoutItem(item_id="item-b", quantity=3),
        )

        first_reserve = await repository.reserve_stock_batch_idempotent(
            items,
            activity_execution_id="act-stock-batch-reserve",
        )
        second_reserve = await repository.reserve_stock_batch_idempotent(
            items,
            activity_execution_id="act-stock-batch-reserve",
        )

        self.assertEqual(first_reserve, second_reserve)
        self.assertEqual(
            first_reserve,
            (
                StockReservation(item_id="item-a", quantity=2),
                StockReservation(item_id="item-b", quantity=3),
            ),
        )
        self.assertEqual((await repository.get_item("item-a")).stock, 3)
        self.assertEqual((await repository.get_item("item-b")).stock, 4)

        first_release = await repository.release_stock_batch_idempotent(
            first_reserve,
            activity_execution_id="act-stock-batch-release",
        )
        second_release = await repository.release_stock_batch_idempotent(
            first_reserve,
            activity_execution_id="act-stock-batch-release",
        )

        self.assertEqual(first_release, second_release)
        self.assertEqual((await repository.get_item("item-a")).stock, 5)
        self.assertEqual((await repository.get_item("item-b")).stock, 7)

    async def test_stock_batch_reserve_is_atomic_on_insufficient_stock(self) -> None:
        repository = StockRepository(self.redis)
        await self.redis.mset(
            {
                "item-a": msgpack.encode(StockValue(stock=5, price=10)),
                "item-b": msgpack.encode(StockValue(stock=1, price=12)),
            }
        )

        with self.assertRaises(StockInsufficientError):
            await repository.reserve_stock_batch_idempotent(
                (
                    CheckoutItem(item_id="item-a", quantity=2),
                    CheckoutItem(item_id="item-b", quantity=3),
                ),
                activity_execution_id="act-stock-batch-fail",
            )

        self.assertEqual((await repository.get_item("item-a")).stock, 5)
        self.assertEqual((await repository.get_item("item-b")).stock, 1)

    async def test_stock_reserve_replays_same_failure_after_state_changes(self) -> None:
        repository = StockRepository(self.redis)
        await self.redis.set("item-2", msgpack.encode(StockValue(stock=1, price=10)))

        with self.assertRaises(StockInsufficientError):
            await repository.reserve_stock_idempotent(
                "item-2",
                quantity=2,
                activity_execution_id="act-stock-2",
            )

        await repository.add_stock("item-2", 10)

        with self.assertRaises(StockInsufficientError):
            await repository.reserve_stock_idempotent(
                "item-2",
                quantity=2,
                activity_execution_id="act-stock-2",
            )

        item = await repository.get_item("item-2")
        self.assertEqual(item.stock, 11)

    async def test_stock_add_stock_accumulates_concurrent_updates(self) -> None:
        repository = StockRepository(self.redis)
        await self.redis.set(
            "item-concurrent",
            msgpack.encode(StockValue(stock=0, price=10)),
        )
        clients = [self.create_redis_client() for _ in range(20)]
        repositories = [StockRepository(client) for client in clients]

        try:
            await asyncio.gather(
                *(repo.add_stock("item-concurrent", 1) for repo in repositories)
            )
        finally:
            await self.close_redis_clients(*clients)

        item = await repository.get_item("item-concurrent")
        self.assertEqual(item.stock, 20)

    async def test_stock_subtract_rejects_concurrent_oversubscription(self) -> None:
        repository = StockRepository(self.redis)
        await self.redis.set(
            "item-oversubscribe",
            msgpack.encode(StockValue(stock=1, price=10)),
        )
        clients = [self.create_redis_client() for _ in range(2)]
        repositories = [StockRepository(client) for client in clients]

        try:
            results = await asyncio.gather(
                *(repo.subtract_stock("item-oversubscribe", 1) for repo in repositories),
                return_exceptions=True,
            )
        finally:
            await self.close_redis_clients(*clients)

        successes = [result for result in results if not isinstance(result, Exception)]
        failures = [result for result in results if isinstance(result, Exception)]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], StockInsufficientError)
        item = await repository.get_item("item-oversubscribe")
        self.assertEqual(item.stock, 0)

    async def test_payment_charge_is_idempotent_across_duplicate_delivery(self) -> None:
        repository = PaymentRepository(self.redis)
        await self.redis.set("user-1", msgpack.encode(UserValue(credit=30)))

        first = await repository.charge_payment_idempotent(
            "user-1",
            amount=20,
            activity_execution_id="act-pay-1",
        )
        second = await repository.charge_payment_idempotent(
            "user-1",
            amount=20,
            activity_execution_id="act-pay-1",
        )

        self.assertEqual(first, second)
        user = await repository.get_user("user-1")
        self.assertEqual(user.credit, 10)

    async def test_payment_charge_replays_same_failure_after_state_changes(self) -> None:
        repository = PaymentRepository(self.redis)
        await self.redis.set("user-2", msgpack.encode(UserValue(credit=5)))

        with self.assertRaises(InsufficientCreditError):
            await repository.charge_payment_idempotent(
                "user-2",
                amount=20,
                activity_execution_id="act-pay-2",
            )

        await repository.add_funds("user-2", 50)

        with self.assertRaises(InsufficientCreditError):
            await repository.charge_payment_idempotent(
                "user-2",
                amount=20,
                activity_execution_id="act-pay-2",
            )

        user = await repository.get_user("user-2")
        self.assertEqual(user.credit, 55)

    async def test_payment_add_funds_accumulates_concurrent_updates(self) -> None:
        repository = PaymentRepository(self.redis)
        await self.redis.set("user-concurrent", msgpack.encode(UserValue(credit=0)))
        clients = [self.create_redis_client() for _ in range(20)]
        repositories = [PaymentRepository(client) for client in clients]

        try:
            await asyncio.gather(
                *(repo.add_funds("user-concurrent", 1) for repo in repositories)
            )
        finally:
            await self.close_redis_clients(*clients)

        user = await repository.get_user("user-concurrent")
        self.assertEqual(user.credit, 20)

    async def test_payment_pay_rejects_concurrent_oversubscription(self) -> None:
        repository = PaymentRepository(self.redis)
        await self.redis.set("user-oversubscribe", msgpack.encode(UserValue(credit=1)))
        clients = [self.create_redis_client() for _ in range(2)]
        repositories = [PaymentRepository(client) for client in clients]

        try:
            results = await asyncio.gather(
                *(repo.pay("user-oversubscribe", 1) for repo in repositories),
                return_exceptions=True,
            )
        finally:
            await self.close_redis_clients(*clients)

        successes = [result for result in results if not isinstance(result, Exception)]
        failures = [result for result in results if isinstance(result, Exception)]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], InsufficientCreditError)
        user = await repository.get_user("user-oversubscribe")
        self.assertEqual(user.credit, 0)

    async def test_mark_order_paid_is_idempotent_across_duplicate_delivery(self) -> None:
        repository = OrderRepository(self.redis)
        await self.redis.set(
            "order-1",
            msgpack.encode(
                OrderValue(
                    paid=False,
                    items=[("item-1", 1), ("item-1", 2)],
                    user_id="user-1",
                    total_cost=30,
                )
            ),
        )

        first = await repository.mark_order_paid_idempotent(
            "order-1",
            activity_execution_id="act-order-1",
        )
        second = await repository.mark_order_paid_idempotent(
            "order-1",
            activity_execution_id="act-order-1",
        )

        self.assertTrue(first.paid)
        self.assertEqual(first, second)
        order = await repository.get_order("order-1")
        self.assertTrue(order.paid)
