from __future__ import annotations

import unittest

import fluxi_sdk_test_support  # noqa: F401

from fluxi_sdk import Worker, activity
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.testing import FakeFluxiRuntime
from order.app.workflows.checkout import OrderCheckoutWorkflow
from shop_common.checkout import (
    CheckoutItem,
    CheckoutOrder,
    PaymentDeclinedError,
    PaymentReceipt,
    StockReservation,
)
from datetime import timedelta


class TestOrderCheckoutWorkflow(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path_marks_order_paid(self) -> None:
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)
        order = CheckoutOrder(
            order_id="order-1",
            user_id="user-1",
            total_cost=20,
            items=(CheckoutItem(item_id="item-1", quantity=2),),
        )
        state = {
            "orders": {"order-1": order},
            "stock": {"item-1": 5},
            "credit": {"user-1": 25},
        }

        @activity.defn(name="reserve_stock_batch")
        async def reserve_stock_batch(
            items: tuple[CheckoutItem, ...],
        ) -> tuple[StockReservation, ...]:
            reservations: list[StockReservation] = []
            for item in items:
                state["stock"][item.item_id] -= item.quantity
                reservations.append(
                    StockReservation(item_id=item.item_id, quantity=item.quantity)
                )
            return tuple(reservations)

        @activity.defn(name="charge_payment")
        async def charge_payment(user_id: str, amount: int) -> PaymentReceipt:
            state["credit"][user_id] -= amount
            return PaymentReceipt(
                payment_id="payment-1",
                user_id=user_id,
                amount=amount,
            )

        @activity.defn(name="mark_order_paid")
        async def mark_order_paid(order_id: str) -> CheckoutOrder:
            state["orders"][order_id].paid = True
            return state["orders"][order_id]

        @activity.defn(name="release_stock_batch")
        async def release_stock_batch(
            reservations: tuple[StockReservation, ...],
        ) -> tuple[StockReservation, ...]:
            for reservation in reservations:
                state["stock"][reservation.item_id] += reservation.quantity
            return reservations

        worker = Worker(
            client,
            task_queue="orders",
            workflows=[OrderCheckoutWorkflow],
            activities=[
                charge_payment,
                mark_order_paid,
            ],
        )
        stock_worker = Worker(
            client,
            task_queue="stock",
            activities=[reserve_stock_batch, release_stock_batch],
        )
        payment_worker = Worker(
            client,
            task_queue="payment",
            activities=[charge_payment],
        )

        async with worker, stock_worker, payment_worker:
            result = await client.execute_workflow(
                OrderCheckoutWorkflow.run,
                order,
                id="checkout:order-1",
                task_queue="orders",
            )

        self.assertEqual(result.status, "paid")
        self.assertTrue(state["orders"]["order-1"].paid)
        self.assertEqual(state["stock"]["item-1"], 3)
        self.assertEqual(state["credit"]["user-1"], 5)
        self.assertEqual(len(runtime.workflow_runs[0].activity_executions), 3)
        self.assertEqual(
            runtime.workflow_runs[0].activity_executions[1].options.schedule_to_close_timeout,
            timedelta(seconds=45),
        )
        self.assertTrue(runtime.workflow_runs[0].activity_executions[2].is_local)
        self.assertEqual(
            runtime.workflow_runs[0].activity_executions[2].activity_name,
            "mark_order_paid",
        )

    async def test_payment_failure_compensates_stock(self) -> None:
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)
        order = CheckoutOrder(
            order_id="order-2",
            user_id="user-2",
            total_cost=20,
            items=(CheckoutItem(item_id="item-2", quantity=2),),
        )
        state = {
            "orders": {"order-2": order},
            "stock": {"item-2": 4},
        }

        @activity.defn(name="reserve_stock_batch")
        async def reserve_stock_batch(
            items: tuple[CheckoutItem, ...],
        ) -> tuple[StockReservation, ...]:
            reservations: list[StockReservation] = []
            for item in items:
                state["stock"][item.item_id] -= item.quantity
                reservations.append(
                    StockReservation(item_id=item.item_id, quantity=item.quantity)
                )
            return tuple(reservations)

        @activity.defn(name="charge_payment")
        async def charge_payment(user_id: str, amount: int) -> PaymentReceipt:
            raise PaymentDeclinedError(
                f"User {user_id!r} insufficient credit: need {amount}, have 5"
            )

        @activity.defn(name="mark_order_paid")
        async def mark_order_paid(order_id: str) -> CheckoutOrder:
            raise AssertionError("mark_order_paid should not run after payment failure")

        @activity.defn(name="release_stock_batch")
        async def release_stock_batch(
            reservations: tuple[StockReservation, ...],
        ) -> tuple[StockReservation, ...]:
            for reservation in reservations:
                state["stock"][reservation.item_id] += reservation.quantity
            return reservations

        worker = Worker(
            client,
            task_queue="orders",
            workflows=[OrderCheckoutWorkflow],
            activities=[mark_order_paid],
        )
        stock_worker = Worker(
            client,
            task_queue="stock",
            activities=[reserve_stock_batch, release_stock_batch],
        )
        payment_worker = Worker(
            client,
            task_queue="payment",
            activities=[charge_payment],
        )

        async with worker, stock_worker, payment_worker:
            result = await client.execute_workflow(
                OrderCheckoutWorkflow.run,
                order,
                id="checkout:order-2",
                task_queue="orders",
            )

        self.assertEqual(result.status, "compensated")
        self.assertEqual(
            result.released_items,
            (StockReservation(item_id="item-2", quantity=2),),
        )
        self.assertEqual(state["stock"]["item-2"], 4)
