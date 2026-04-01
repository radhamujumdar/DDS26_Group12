from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any

from fluxi_sdk import activity, workflow
from shop_common.checkout import (
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    StockReservation,
)

from ..services.order_service import OrderService


@workflow.defn(name="OrderCheckoutWorkflow")
class OrderCheckoutWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> CheckoutWorkflowResult:
        order = await workflow.execute_activity("load_order", order_id)
        reservations: list[StockReservation] = []

        for item in order.items:
            reservation = await workflow.execute_activity(
                "reserve_stock",
                item.item_id,
                item.quantity,
                task_queue="stock",
                schedule_to_close_timeout=timedelta(seconds=30),
            )
            reservations.append(reservation)

        try:
            payment = await workflow.execute_activity(
                "charge_payment",
                order.user_id,
                order.total_cost,
                task_queue="payment",
                schedule_to_close_timeout=timedelta(seconds=30),
            )
        except PaymentDeclinedError as exc:
            released: list[StockReservation] = []
            for reservation in reversed(reservations):
                released_item = await workflow.execute_activity(
                    "release_stock",
                    reservation.item_id,
                    reservation.quantity,
                    task_queue="stock",
                )
                released.append(released_item)
            return CheckoutWorkflowResult(
                order_id=order.order_id,
                status="compensated",
                released_items=tuple(released),
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
) -> tuple[Callable[..., Any], Callable[..., Any]]:
    @activity.defn(name="load_order")
    async def load_order(order_id: str):
        return await order_service.load_checkout_order(order_id)

    @activity.defn(name="mark_order_paid")
    async def mark_order_paid(order_id: str):
        return await order_service.mark_order_paid(
            order_id,
            activity_execution_id=activity.info().activity_execution_id,
        )

    return load_order, mark_order_paid
