import unittest

from fluxi_sdk.examples.checkout import (
    CheckoutItem,
    CheckoutOrder,
    PaymentReceipt,
    ReferenceCheckoutState,
    ReferenceCheckoutWorkflow,
    StockReservation,
    create_reference_checkout_runtime,
)


class TestReferenceCheckoutWorkflow(unittest.IsolatedAsyncioTestCase):

    async def test_reference_checkout_happy_path(self):
        state = ReferenceCheckoutState(
            orders={
                "order-1": CheckoutOrder(
                    order_id="order-1",
                    user_id="user-1",
                    total_cost=20,
                    items=(CheckoutItem(item_id="item-1", quantity=2),),
                ),
            },
            stock_levels={"item-1": 5},
            user_credit={"user-1": 25},
        )
        runtime = create_reference_checkout_runtime(state)
        client = runtime.create_client()

        result = await client.execute_workflow(
            ReferenceCheckoutWorkflow,
            workflow_key="checkout:order-1",
            args=("order-1",),
        )

        self.assertEqual(result.order_id, "order-1")
        self.assertEqual(result.status, "paid")
        self.assertEqual(result.failure_reason, None)
        self.assertEqual(result.released_items, ())
        self.assertEqual(
            result.payment,
            PaymentReceipt(payment_id="payment-1", user_id="user-1", amount=20),
        )

        self.assertTrue(state.orders["order-1"].paid)
        self.assertEqual(state.stock_levels["item-1"], 3)
        self.assertEqual(state.user_credit["user-1"], 5)
        self.assertEqual(
            runtime.workflow_runs[0].result,
            result,
        )
        self.assertEqual(
            [execution.activity_name for execution in runtime.workflow_runs[0].activity_executions],
            ["load_order", "reserve_stock", "charge_payment", "mark_order_paid"],
        )
        self.assertEqual(
            [execution.options.task_queue for execution in runtime.workflow_runs[0].activity_executions],
            ["orders", "stock", "payment", "orders"],
        )

    async def test_reference_checkout_compensates_after_payment_failure(self):
        state = ReferenceCheckoutState(
            orders={
                "order-2": CheckoutOrder(
                    order_id="order-2",
                    user_id="user-2",
                    total_cost=20,
                    items=(CheckoutItem(item_id="item-2", quantity=2),),
                ),
            },
            stock_levels={"item-2": 4},
            user_credit={"user-2": 5},
        )
        runtime = create_reference_checkout_runtime(state)
        client = runtime.create_client()

        result = await client.execute_workflow(
            "ReferenceCheckoutWorkflow",
            workflow_key="checkout:order-2",
            args=("order-2",),
        )

        self.assertEqual(result.order_id, "order-2")
        self.assertEqual(result.status, "compensated")
        self.assertIsNone(result.payment)
        self.assertIn("insufficient credit", result.failure_reason)
        self.assertEqual(
            result.released_items,
            (StockReservation(item_id="item-2", quantity=2),),
        )

        self.assertFalse(state.orders["order-2"].paid)
        self.assertEqual(state.stock_levels["item-2"], 4)
        self.assertEqual(state.user_credit["user-2"], 5)
        self.assertEqual(state.released_items, [StockReservation(item_id="item-2", quantity=2)])
        self.assertEqual(
            [execution.activity_name for execution in runtime.workflow_runs[0].activity_executions],
            ["load_order", "reserve_stock", "charge_payment", "release_stock"],
        )
        self.assertEqual(
            [execution.options.task_queue for execution in runtime.workflow_runs[0].activity_executions],
            ["orders", "stock", "payment", "stock"],
        )


if __name__ == "__main__":
    unittest.main()
