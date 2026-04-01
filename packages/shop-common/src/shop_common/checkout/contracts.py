"""Shared checkout dataclasses and business exceptions."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass


class CheckoutError(Exception):
    """Base error for checkout workflow/activity failures."""


class OrderNotFoundError(CheckoutError):
    """Raised when the requested order does not exist."""


class StockUnavailableError(CheckoutError):
    """Raised when stock cannot be reserved for the order."""


class PaymentDeclinedError(CheckoutError):
    """Raised when the payment activity declines the charge."""


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


def coalesce_order_items(items: Iterable[tuple[str, int]]) -> tuple[CheckoutItem, ...]:
    quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        quantities[item_id] += quantity
    return tuple(
        CheckoutItem(item_id=item_id, quantity=quantity)
        for item_id, quantity in quantities.items()
    )
