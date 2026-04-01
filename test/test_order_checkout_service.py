from __future__ import annotations

from dataclasses import dataclass
import unittest
from unittest.mock import AsyncMock

import fluxi_sdk_test_support  # noqa: F401

from fluxi_sdk.types import StartPolicy
from order.app.services.checkout_service import CheckoutService
from shop_common.checkout import (
    CheckoutItem,
    CheckoutOrder,
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    PaymentReceipt,
)


@dataclass
class _ClientStub:
    execute_workflow: AsyncMock


@dataclass
class _OrderServiceStub:
    load_checkout_order: AsyncMock


class TestCheckoutService(unittest.IsolatedAsyncioTestCase):
    async def test_checkout_uses_allow_duplicate_start_policy(self) -> None:
        client = _ClientStub(
            execute_workflow=AsyncMock(
                return_value=CheckoutWorkflowResult(
                    order_id="order-1",
                    status="paid",
                    payment=PaymentReceipt(
                        payment_id="payment-1",
                        user_id="user-1",
                        amount=20,
                    ),
                )
            )
        )
        order = CheckoutOrder(
            order_id="order-1",
            user_id="user-1",
            total_cost=20,
            items=(CheckoutItem(item_id="item-1", quantity=2),),
        )
        order_service = _OrderServiceStub(load_checkout_order=AsyncMock(return_value=order))
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        result = await service.checkout("order-1")

        self.assertEqual(result.status, "paid")
        order_service.load_checkout_order.assert_awaited_once_with("order-1")
        client.execute_workflow.assert_awaited_once()
        args, kwargs = client.execute_workflow.await_args
        self.assertEqual(args[1], order)
        self.assertEqual(kwargs["id"], "order-checkout:order-1")
        self.assertEqual(kwargs["task_queue"], "orders")
        self.assertEqual(kwargs["start_policy"], StartPolicy.ALLOW_DUPLICATE)

    async def test_checkout_maps_compensated_result_to_payment_declined(self) -> None:
        client = _ClientStub(
            execute_workflow=AsyncMock(
                return_value=CheckoutWorkflowResult(
                    order_id="order-2",
                    status="compensated",
                    failure_reason="User 'user-2' insufficient credit: need 20, have 5",
                )
            )
        )
        order = CheckoutOrder(
            order_id="order-2",
            user_id="user-2",
            total_cost=20,
            items=(CheckoutItem(item_id="item-2", quantity=2),),
        )
        order_service = _OrderServiceStub(load_checkout_order=AsyncMock(return_value=order))
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        with self.assertRaises(PaymentDeclinedError):
            await service.checkout("order-2")
