"""Shared checkout contracts and domain exceptions."""

from .contracts import (
    CheckoutError,
    CheckoutItem,
    CheckoutOrder,
    CheckoutWorkflowResult,
    OrderNotFoundError,
    PaymentDeclinedError,
    PaymentReceipt,
    StockReservation,
    StockUnavailableError,
    coalesce_order_items,
)

__all__ = [
    "CheckoutError",
    "CheckoutItem",
    "CheckoutOrder",
    "CheckoutWorkflowResult",
    "OrderNotFoundError",
    "PaymentDeclinedError",
    "PaymentReceipt",
    "StockReservation",
    "StockUnavailableError",
    "coalesce_order_items",
]
