from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Callable, Protocol

from flask import Response, abort

from fluxi_sdk import activity
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.examples.checkout import (
    CheckoutItem,
    CheckoutOrder,
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    PaymentReceipt,
    ReferenceCheckoutWorkflow,
    StockReservation,
    StockUnavailableError,
)
from fluxi_sdk.testing import FakeFluxiRuntime
from fluxi_sdk.worker import Worker


class CheckoutCoordinator(Protocol):
    def checkout(self, order_id: str) -> Response:
        ...


class LegacyCheckoutCoordinator:
    def __init__(
        self,
        *,
        gateway_url: str,
        get_order: Callable[[str], Any],
        save_order: Callable[[str, Any], None],
        send_post_request: Callable[[str], Any],
        logger: Any,
    ) -> None:
        self._gateway_url = gateway_url
        self._get_order = get_order
        self._save_order = save_order
        self._send_post_request = send_post_request
        self._logger = logger

    def checkout(self, order_id: str) -> Response:
        self._logger.debug("Checking out %s", order_id)
        order_entry = self._get_order(order_id)
        removed_items: list[tuple[str, int]] = []

        for item_id, quantity in _coalesce_order_items(order_entry.items).items():
            stock_reply = self._send_post_request(
                f"{self._gateway_url}/stock/subtract/{item_id}/{quantity}"
            )
            if stock_reply.status_code != 200:
                self._rollback_stock(removed_items)
                abort(400, f"Out of stock on item_id: {item_id}")
            removed_items.append((item_id, quantity))

        user_reply = self._send_post_request(
            f"{self._gateway_url}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"
        )
        if user_reply.status_code != 200:
            self._rollback_stock(removed_items)
            abort(400, "User out of credit")

        order_entry.paid = True
        self._save_order(order_id, order_entry)
        self._logger.debug("Checkout successful")
        return Response("Checkout successful", status=200)

    def _rollback_stock(self, removed_items: list[tuple[str, int]]) -> None:
        for item_id, quantity in removed_items:
            self._send_post_request(f"{self._gateway_url}/stock/add/{item_id}/{quantity}")


class FluxiCheckoutCoordinator:
    def __init__(
        self,
        *,
        gateway_url: str,
        get_order: Callable[[str], Any],
        save_order: Callable[[str, Any], None],
        send_post_request: Callable[[str], Any],
        logger: Any,
    ) -> None:
        self._gateway_url = gateway_url
        self._get_order = get_order
        self._save_order = save_order
        self._send_post_request = send_post_request
        self._logger = logger

    def checkout(self, order_id: str) -> Response:
        self._logger.debug("Checking out %s via Fluxi", order_id)

        try:
            result = asyncio.run(self._execute_checkout(order_id))
        except StockUnavailableError as exc:
            abort(400, str(exc))
        except PaymentDeclinedError:
            abort(400, "User out of credit")

        if result.status == "paid":
            self._logger.debug("Checkout successful")
            return Response("Checkout successful", status=200)

        if result.status == "compensated":
            abort(400, "User out of credit")

        abort(500, f"Unsupported checkout result status: {result.status}")

    async def _execute_checkout(self, order_id: str) -> CheckoutWorkflowResult:
        runtime = FakeFluxiRuntime()
        client = WorkflowClient.connect(runtime=runtime)
        workers = self._create_reference_checkout_workers(client, order_id)

        async with workers[0], workers[1], workers[2]:
            return await client.execute_workflow(
                ReferenceCheckoutWorkflow.run,
                order_id,
                id=f"order-checkout:{order_id}",
                task_queue="orders",
            )

    def _create_reference_checkout_workers(
        self,
        client: WorkflowClient,
        order_id: str,
    ) -> tuple[Worker, Worker, Worker]:
        @activity.defn(name="load_order")
        def load_order(loaded_order_id: str) -> CheckoutOrder:
            order_entry = self._get_order(loaded_order_id)
            return _to_checkout_order(loaded_order_id, order_entry)

        @activity.defn(name="reserve_stock")
        def reserve_stock(item_id: str, quantity: int) -> StockReservation:
            stock_reply = self._send_post_request(
                f"{self._gateway_url}/stock/subtract/{item_id}/{quantity}"
            )
            if stock_reply.status_code != 200:
                raise StockUnavailableError(f"Out of stock on item_id: {item_id}")
            return StockReservation(item_id=item_id, quantity=quantity)

        @activity.defn(name="charge_payment")
        def charge_payment(user_id: str, amount: int) -> PaymentReceipt:
            payment_reply = self._send_post_request(
                f"{self._gateway_url}/payment/pay/{user_id}/{amount}"
            )
            if payment_reply.status_code != 200:
                raise PaymentDeclinedError("User out of credit")
            return PaymentReceipt(
                payment_id=f"payment:{order_id}",
                user_id=user_id,
                amount=amount,
            )

        @activity.defn(name="mark_order_paid")
        def mark_order_paid(paid_order_id: str) -> CheckoutOrder:
            order_entry = self._get_order(paid_order_id)
            order_entry.paid = True
            self._save_order(paid_order_id, order_entry)
            return _to_checkout_order(paid_order_id, order_entry)

        @activity.defn(name="release_stock")
        def release_stock(item_id: str, quantity: int) -> StockReservation:
            self._send_post_request(f"{self._gateway_url}/stock/add/{item_id}/{quantity}")
            return StockReservation(item_id=item_id, quantity=quantity)

        orders_worker = Worker(
            client,
            task_queue="orders",
            workflows=[ReferenceCheckoutWorkflow],
            activities=[load_order, mark_order_paid],
        )
        stock_worker = Worker(
            client,
            task_queue="stock",
            activities=[reserve_stock, release_stock],
        )
        payment_worker = Worker(
            client,
            task_queue="payment",
            activities=[charge_payment],
        )
        return orders_worker, stock_worker, payment_worker


def build_checkout_coordinator(
    *,
    mode: str,
    gateway_url: str,
    get_order: Callable[[str], Any],
    save_order: Callable[[str, Any], None],
    send_post_request: Callable[[str], Any],
    logger: Any,
) -> CheckoutCoordinator:
    if mode == "legacy":
        return LegacyCheckoutCoordinator(
            gateway_url=gateway_url,
            get_order=get_order,
            save_order=save_order,
            send_post_request=send_post_request,
            logger=logger,
        )
    if mode == "fluxi":
        return FluxiCheckoutCoordinator(
            gateway_url=gateway_url,
            get_order=get_order,
            save_order=save_order,
            send_post_request=send_post_request,
            logger=logger,
        )
    raise ValueError(
        "ORDER_CHECKOUT_COORDINATOR must be either 'legacy' or 'fluxi'."
    )


def _coalesce_order_items(items: list[tuple[str, int]]) -> dict[str, int]:
    quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        quantities[item_id] += quantity
    return quantities


def _to_checkout_order(order_id: str, order_entry: Any) -> CheckoutOrder:
    items = tuple(
        CheckoutItem(item_id=item_id, quantity=quantity)
        for item_id, quantity in _coalesce_order_items(order_entry.items).items()
    )
    return CheckoutOrder(
        order_id=order_id,
        user_id=order_entry.user_id,
        total_cost=order_entry.total_cost,
        items=items,
        paid=order_entry.paid,
    )
