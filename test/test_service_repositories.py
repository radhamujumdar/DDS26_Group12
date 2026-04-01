from __future__ import annotations

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


class TestServiceRepositories(RepositoryRedisAsyncTestCase):
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
