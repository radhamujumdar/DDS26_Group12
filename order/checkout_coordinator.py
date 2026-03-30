from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from typing import Any, Callable, Protocol

from flask import Response, abort

from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.examples.checkout import (
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    ReferenceCheckoutWorkflow,
    StockUnavailableError,
)


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
    """Submits checkout as a Fluxi workflow and waits for the terminal result.

    Workers (order/workers/checkout_worker.py, stock/workers/stock_worker.py,
    payment/workers/payment_worker.py) run as separate processes and poll the
    fluxi-server for tasks. This coordinator only submits the workflow.
    """

    def __init__(self, *, client: WorkflowClient, logger: Any) -> None:
        self._client = client
        self._logger = logger

    def checkout(self, order_id: str) -> Response:
        self._logger.debug("Checking out %s via Fluxi", order_id)

        try:
            result: CheckoutWorkflowResult = asyncio.run(
                self._client.execute_workflow(
                    ReferenceCheckoutWorkflow.run,
                    order_id,
                    id=f"order-checkout:{order_id}",
                    task_queue="orders",
                )
            )
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
        from fluxi_sdk.runtime.redis import RedisFluxiRuntime  # noqa: PLC0415

        runtime = RedisFluxiRuntime(redis_url=os.environ["FLUXI_REDIS_URL"])
        client = WorkflowClient.connect(runtime=runtime)
        return FluxiCheckoutCoordinator(client=client, logger=logger)
    raise ValueError(
        "ORDER_CHECKOUT_COORDINATOR must be either 'legacy' or 'fluxi'."
    )


def _coalesce_order_items(items: list[tuple[str, int]]) -> dict[str, int]:
    quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        quantities[item_id] += quantity
    return quantities
