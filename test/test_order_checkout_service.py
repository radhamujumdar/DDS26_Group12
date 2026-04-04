from __future__ import annotations

from dataclasses import dataclass
import unittest
from unittest.mock import AsyncMock, patch

import fluxi_sdk_test_support  # noqa: F401

from fluxi_sdk.errors import RemoteWorkflowError
import order.app.services.checkout_service as checkout_service_module
from fluxi_sdk.types import StartPolicy
from order.app.services.checkout_service import CheckoutService
from order.app.services.order_service import CheckoutPreparation
from shop_common.checkout import (
    CheckoutItem,
    CheckoutOrder,
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    PaymentReceipt,
)
from shop_common.errors import DatabaseError, UpstreamServiceError


@dataclass
class _ClientStub:
    execute_workflow: AsyncMock


@dataclass
class _OrderServiceStub:
    prepare_checkout: AsyncMock
    complete_checkout_attempt: AsyncMock


class TestCheckoutService(unittest.IsolatedAsyncioTestCase):
    async def test_checkout_uses_attach_or_start_per_attempt(self) -> None:
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
        order_service = _OrderServiceStub(
            prepare_checkout=AsyncMock(
                return_value=CheckoutPreparation(
                    order=order,
                    attempt_no=3,
                    already_paid=False,
                )
            ),
            complete_checkout_attempt=AsyncMock(),
        )
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        result = await service.checkout("order-1")

        self.assertEqual(result.status, "paid")
        order_service.prepare_checkout.assert_awaited_once_with("order-1")
        order_service.complete_checkout_attempt.assert_awaited_once_with(
            "order-1",
            attempt_no=3,
            status="paid",
        )
        client.execute_workflow.assert_awaited_once()
        args, kwargs = client.execute_workflow.await_args
        self.assertEqual(args[1], order)
        self.assertEqual(kwargs["id"], "order-checkout:order-1:attempt:3")
        self.assertEqual(kwargs["task_queue"], "orders")
        self.assertEqual(kwargs["start_policy"], StartPolicy.ATTACH_OR_START)

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
        order_service = _OrderServiceStub(
            prepare_checkout=AsyncMock(
                return_value=CheckoutPreparation(
                    order=order,
                    attempt_no=1,
                    already_paid=False,
                )
            ),
            complete_checkout_attempt=AsyncMock(),
        )
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        with self.assertRaises(PaymentDeclinedError):
            await service.checkout("order-2")

        order_service.complete_checkout_attempt.assert_awaited_once_with(
            "order-2",
            attempt_no=1,
            status="compensated",
        )

    async def test_checkout_short_circuits_when_order_is_already_paid(self) -> None:
        client = _ClientStub(execute_workflow=AsyncMock())
        order = CheckoutOrder(
            order_id="order-3",
            user_id="user-3",
            total_cost=20,
            items=(CheckoutItem(item_id="item-3", quantity=2),),
            paid=True,
        )
        order_service = _OrderServiceStub(
            prepare_checkout=AsyncMock(
                return_value=CheckoutPreparation(
                    order=order,
                    attempt_no=None,
                    already_paid=True,
                )
            ),
            complete_checkout_attempt=AsyncMock(),
        )
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        result = await service.checkout("order-3")

        self.assertEqual(result.status, "paid")
        client.execute_workflow.assert_not_awaited()
        order_service.complete_checkout_attempt.assert_not_awaited()

    async def test_checkout_maps_remote_workflow_failures_to_upstream_service_error(self) -> None:
        client = _ClientStub(
            execute_workflow=AsyncMock(
                side_effect=RemoteWorkflowError("Fluxi store temporarily unavailable.")
            )
        )
        order = CheckoutOrder(
            order_id="order-4",
            user_id="user-4",
            total_cost=20,
            items=(CheckoutItem(item_id="item-4", quantity=2),),
        )
        order_service = _OrderServiceStub(
            prepare_checkout=AsyncMock(
                return_value=CheckoutPreparation(
                    order=order,
                    attempt_no=1,
                    already_paid=False,
                )
            ),
            complete_checkout_attempt=AsyncMock(),
        )
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        with self.assertRaises(UpstreamServiceError):
            await service.checkout("order-4")

        order_service.complete_checkout_attempt.assert_not_awaited()

    async def test_checkout_retries_decode_errors_and_reuses_same_attempt(self) -> None:
        client = _ClientStub(
            execute_workflow=AsyncMock(
                side_effect=[
                    TypeError("Failure payload is not a valid Fluxi failure envelope."),
                    CheckoutWorkflowResult(
                        order_id="order-6",
                        status="paid",
                        payment=PaymentReceipt(
                            payment_id="payment-6",
                            user_id="user-6",
                            amount=20,
                        ),
                    ),
                ]
            )
        )
        order = CheckoutOrder(
            order_id="order-6",
            user_id="user-6",
            total_cost=20,
            items=(CheckoutItem(item_id="item-6", quantity=2),),
        )
        order_service = _OrderServiceStub(
            prepare_checkout=AsyncMock(
                return_value=CheckoutPreparation(
                    order=order,
                    attempt_no=2,
                    already_paid=False,
                )
            ),
            complete_checkout_attempt=AsyncMock(),
        )
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        with patch.object(
            checkout_service_module,
            "_WORKFLOW_EXECUTION_RETRY_INITIAL_DELAY_SECONDS",
            0.0,
        ), patch.object(
            checkout_service_module,
            "_WORKFLOW_EXECUTION_RETRY_MAX_DELAY_SECONDS",
            0.0,
        ):
            result = await service.checkout("order-6")

        self.assertEqual(result.status, "paid")
        self.assertEqual(client.execute_workflow.await_count, 2)
        first_call = client.execute_workflow.await_args_list[0]
        second_call = client.execute_workflow.await_args_list[1]
        self.assertEqual(first_call.kwargs["id"], "order-checkout:order-6:attempt:2")
        self.assertEqual(second_call.kwargs["id"], "order-checkout:order-6:attempt:2")
        order_service.complete_checkout_attempt.assert_awaited_once_with(
            "order-6",
            attempt_no=2,
            status="paid",
        )

    async def test_checkout_maps_prepare_checkout_database_failures_to_upstream_service_error(self) -> None:
        client = _ClientStub(execute_workflow=AsyncMock())
        order_service = _OrderServiceStub(
            prepare_checkout=AsyncMock(side_effect=DatabaseError("DB error")),
            complete_checkout_attempt=AsyncMock(),
        )
        service = CheckoutService(client=client, order_service=order_service)  # type: ignore[arg-type]

        with self.assertRaises(UpstreamServiceError):
            await service.checkout("order-5")

        client.execute_workflow.assert_not_awaited()
