from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from fluxi_sdk import activity, workflow
from shop_common.checkout import (
    CheckoutOrder,
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    StockReservation,
)

from ..services.order_service import OrderService


@workflow.defn(name="OrderCheckoutWorkflow")
class OrderCheckoutWorkflow:
    @workflow.run
    async def run(self, order: CheckoutOrder) -> CheckoutWorkflowResult:
        reservations: tuple[StockReservation, ...] = ()
        if order.items:
            reservations = await workflow.execute_activity(
                "reserve_stock_batch",
                order.items,
                task_queue="stock",
                schedule_to_close_timeout=timedelta(seconds=30),
            )

        try:
            payment = await workflow.execute_activity(
                "charge_payment",
                order.user_id,
                order.total_cost,
                task_queue="payment",
                schedule_to_close_timeout=timedelta(seconds=30),
            )
        except PaymentDeclinedError as exc:
            released: tuple[StockReservation, ...] = ()
            if reservations:
                released = await workflow.execute_activity(
                    "release_stock_batch",
                    reservations,
                    task_queue="stock",
                )
            return CheckoutWorkflowResult(
                order_id=order.order_id,
                status="compensated",
                released_items=released,
                failure_reason=str(exc),
            )

        await workflow.execute_activity("mark_order_paid", order.order_id)
        return CheckoutWorkflowResult(
            order_id=order.order_id,
            status="paid",
            payment=payment,
        )


def create_order_activities(
    order_service: OrderService,
) -> tuple[Callable[..., Any]]:
    @activity.defn(name="mark_order_paid")
    async def mark_order_paid(order_id: str):
        return await order_service.mark_order_paid(
            order_id,
            activity_execution_id=activity.info().activity_execution_id,
        )

    return (mark_order_paid,)
