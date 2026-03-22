"""Reference checkout workflow and fake activities for phase 1."""

from __future__ import annotations

from dataclasses import dataclass, field

from .. import activity, workflow
from ..testing import FakeFluxiRuntime


class ReferenceCheckoutError(Exception):
    """Base error for the reference checkout example."""


class OrderNotFoundError(ReferenceCheckoutError):
    """Raised when a requested order does not exist in the fake state."""


class StockUnavailableError(ReferenceCheckoutError):
    """Raised when the fake stock service cannot reserve the requested quantity."""


class PaymentDeclinedError(ReferenceCheckoutError):
    """Raised when the fake payment service declines a charge."""


@dataclass(frozen=True, slots=True)
class CheckoutItem:
    item_id: str
    quantity: int


@dataclass(slots=True)
class CheckoutOrder:
    order_id: str
    user_id: str
    total_cost: int
    items: tuple[CheckoutItem, ...]
    paid: bool = False


@dataclass(frozen=True, slots=True)
class StockReservation:
    item_id: str
    quantity: int


@dataclass(frozen=True, slots=True)
class PaymentReceipt:
    payment_id: str
    user_id: str
    amount: int


@dataclass(frozen=True, slots=True)
class CheckoutWorkflowResult:
    order_id: str
    status: str
    payment: PaymentReceipt | None = None
    released_items: tuple[StockReservation, ...] = ()
    failure_reason: str | None = None


@dataclass(slots=True)
class ReferenceCheckoutState:
    orders: dict[str, CheckoutOrder] = field(default_factory=dict)
    stock_levels: dict[str, int] = field(default_factory=dict)
    user_credit: dict[str, int] = field(default_factory=dict)
    payment_receipts: list[PaymentReceipt] = field(default_factory=list)
    released_items: list[StockReservation] = field(default_factory=list)
    next_payment_number: int = 1


@workflow.defn(name="ReferenceCheckoutWorkflow", default_task_queue="orders")
class ReferenceCheckoutWorkflow:
    """Sample checkout workflow using Fluxi's public SDK surface."""

    @workflow.run
    async def run(self, order_id: str) -> CheckoutWorkflowResult:
        order = await workflow.execute_activity("load_order", args=(order_id,))
        reservations: list[StockReservation] = []

        for item in order.items:
            reservation = await workflow.execute_activity(
                "reserve_stock",
                args=(item.item_id, item.quantity),
                task_queue="stock",
            )
            reservations.append(reservation)

        try:
            payment = await workflow.execute_activity(
                "charge_payment",
                args=(order.user_id, order.total_cost),
                task_queue="payment",
            )
        except PaymentDeclinedError as exc:
            released: list[StockReservation] = []
            for reservation in reversed(reservations):
                released_item = await workflow.execute_activity(
                    "release_stock",
                    args=(reservation.item_id, reservation.quantity),
                    task_queue="stock",
                )
                released.append(released_item)
            return CheckoutWorkflowResult(
                order_id=order.order_id,
                status="compensated",
                released_items=tuple(released),
                failure_reason=str(exc),
            )

        await workflow.execute_activity("mark_order_paid", args=(order.order_id,))
        return CheckoutWorkflowResult(
            order_id=order.order_id,
            status="paid",
            payment=payment,
        )


def register_reference_checkout_activities(
    runtime: FakeFluxiRuntime,
    state: ReferenceCheckoutState,
) -> None:
    """Register fake in-memory activities that back the reference workflow."""

    @activity.defn(name="load_order")
    def load_order(order_id: str) -> CheckoutOrder:
        order = state.orders.get(order_id)
        if order is None:
            raise OrderNotFoundError(f"Order {order_id!r} was not found.")
        return order

    @activity.defn(name="reserve_stock")
    def reserve_stock(item_id: str, quantity: int) -> StockReservation:
        available = state.stock_levels.get(item_id, 0)
        if available < quantity:
            raise StockUnavailableError(
                f"Item {item_id!r} has insufficient stock for quantity {quantity}."
            )
        state.stock_levels[item_id] = available - quantity
        return StockReservation(item_id=item_id, quantity=quantity)

    @activity.defn(name="charge_payment")
    def charge_payment(user_id: str, amount: int) -> PaymentReceipt:
        credit = state.user_credit.get(user_id, 0)
        if credit < amount:
            raise PaymentDeclinedError(
                f"User {user_id!r} has insufficient credit for amount {amount}."
            )

        state.user_credit[user_id] = credit - amount
        receipt = PaymentReceipt(
            payment_id=f"payment-{state.next_payment_number}",
            user_id=user_id,
            amount=amount,
        )
        state.next_payment_number += 1
        state.payment_receipts.append(receipt)
        return receipt

    @activity.defn(name="mark_order_paid")
    def mark_order_paid(order_id: str) -> CheckoutOrder:
        order = state.orders.get(order_id)
        if order is None:
            raise OrderNotFoundError(f"Order {order_id!r} was not found.")
        order.paid = True
        return order

    @activity.defn(name="release_stock")
    def release_stock(item_id: str, quantity: int) -> StockReservation:
        state.stock_levels[item_id] = state.stock_levels.get(item_id, 0) + quantity
        released = StockReservation(item_id=item_id, quantity=quantity)
        state.released_items.append(released)
        return released

    runtime.register_activity(load_order)
    runtime.register_activity(reserve_stock)
    runtime.register_activity(charge_payment)
    runtime.register_activity(mark_order_paid)
    runtime.register_activity(release_stock)


def register_reference_checkout(
    runtime: FakeFluxiRuntime,
    state: ReferenceCheckoutState,
) -> FakeFluxiRuntime:
    """Register the reference workflow and fake activities into a runtime."""

    runtime.register_workflow(ReferenceCheckoutWorkflow)
    register_reference_checkout_activities(runtime, state)
    return runtime


def create_reference_checkout_runtime(
    state: ReferenceCheckoutState,
) -> FakeFluxiRuntime:
    """Create a fake runtime preloaded with the reference checkout workflow."""

    runtime = FakeFluxiRuntime()
    return register_reference_checkout(runtime, state)


__all__ = [
    "CheckoutItem",
    "CheckoutOrder",
    "CheckoutWorkflowResult",
    "OrderNotFoundError",
    "PaymentDeclinedError",
    "PaymentReceipt",
    "ReferenceCheckoutError",
    "ReferenceCheckoutState",
    "ReferenceCheckoutWorkflow",
    "StockReservation",
    "StockUnavailableError",
    "create_reference_checkout_runtime",
    "register_reference_checkout",
    "register_reference_checkout_activities",
]
