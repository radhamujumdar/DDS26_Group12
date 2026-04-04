from __future__ import annotations

import logging
import time

from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.types import StartPolicy
from fluxi_engine.observability import elapsed_ms, trace_logging_enabled
from shop_common.checkout import CheckoutWorkflowResult, PaymentDeclinedError, StockUnavailableError

from .order_service import OrderService
from ..workflows.checkout import OrderCheckoutWorkflow


logger = logging.getLogger(__name__)
TRACE_LOGGING_ENABLED = trace_logging_enabled()


class CheckoutService:
    def __init__(self, client: WorkflowClient, order_service: OrderService) -> None:
        self._client = client
        self._order_service = order_service

    async def checkout(self, order_id: str) -> CheckoutWorkflowResult:
        request_start = time.perf_counter()
        if TRACE_LOGGING_ENABLED:
            logger.info("checkout.request.start order_id=%s", order_id)

        preparation_start = time.perf_counter()
        preparation = await self._order_service.prepare_checkout(order_id)
        order = preparation.order
        if preparation.already_paid:
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "checkout.request.already_paid order_id=%s duration_ms=%.2f",
                    order_id,
                    elapsed_ms(request_start),
                )
            return CheckoutWorkflowResult(order_id=order_id, status="paid")

        attempt_no = preparation.attempt_no
        if attempt_no is None:
            raise RuntimeError("Checkout attempt number must be present for unpaid orders.")
        workflow_id = self._workflow_id(order_id, attempt_no)
        if TRACE_LOGGING_ENABLED:
            logger.info(
                "checkout.request.prepared order_id=%s workflow_id=%s attempt_no=%d items=%d total_cost=%d paid=%s duration_ms=%.2f",
                order_id,
                workflow_id,
                attempt_no,
                len(order.items),
                order.total_cost,
                order.paid,
                elapsed_ms(preparation_start),
            )

        workflow_wait_start = time.perf_counter()
        result: CheckoutWorkflowResult = await self._client.execute_workflow(
            OrderCheckoutWorkflow.run,
            order,
            id=workflow_id,
            task_queue="orders",
            start_policy=StartPolicy.ATTACH_OR_START,
        )
        await self._order_service.complete_checkout_attempt(
            order_id,
            attempt_no=attempt_no,
            status=result.status,
        )
        if TRACE_LOGGING_ENABLED:
            logger.info(
                "checkout.request.workflow_done order_id=%s workflow_id=%s attempt_no=%d status=%s workflow_wait_ms=%.2f total_duration_ms=%.2f",
                order_id,
                workflow_id,
                attempt_no,
                result.status,
                elapsed_ms(workflow_wait_start),
                elapsed_ms(request_start),
            )
        if result.status == "paid":
            return result
        if result.status == "compensated":
            raise PaymentDeclinedError(result.failure_reason or "User out of credit")
        raise RuntimeError(f"Unsupported checkout result status: {result.status}")

    @staticmethod
    def to_http_response_text(result: CheckoutWorkflowResult) -> str:
        if result.status != "paid":
            raise RuntimeError(f"Unsupported terminal checkout status: {result.status}")
        return "Checkout successful"

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _workflow_id(order_id: str, attempt_no: int) -> str:
        return f"order-checkout:{order_id}:attempt:{attempt_no}"
