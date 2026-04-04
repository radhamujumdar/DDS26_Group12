from __future__ import annotations

import asyncio
import logging
import time

import httpx
from redis.asyncio.sentinel import MasterNotFoundError
from redis.exceptions import ConnectionError, ReadOnlyError, ResponseError, TimeoutError

from fluxi_sdk.errors import RemoteWorkflowError
from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.types import StartPolicy
from fluxi_engine.observability import elapsed_ms, trace_logging_enabled
from shop_common.checkout import CheckoutWorkflowResult, PaymentDeclinedError, StockUnavailableError
from shop_common.errors import DatabaseError, UpstreamServiceError

from .order_service import OrderService
from ..workflows.checkout import OrderCheckoutWorkflow


logger = logging.getLogger(__name__)
TRACE_LOGGING_ENABLED = trace_logging_enabled()
_FAILOVER_LOADING_MARKERS = ("LOADING", "MASTERDOWN", "TRYAGAIN")
_WORKFLOW_EXECUTION_RETRY_TIMEOUT_SECONDS = 5.0
_WORKFLOW_EXECUTION_RETRY_INITIAL_DELAY_SECONDS = 0.2
_WORKFLOW_EXECUTION_RETRY_MAX_DELAY_SECONDS = 1.0


def _is_transient_checkout_backend_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            DatabaseError,
            UpstreamServiceError,
            RemoteWorkflowError,
            httpx.HTTPError,
            ConnectionError,
            TimeoutError,
            MasterNotFoundError,
            ReadOnlyError,
        ),
    ):
        return True
    if isinstance(exc, ResponseError):
        message = str(exc).upper()
        return any(marker in message for marker in _FAILOVER_LOADING_MARKERS)
    return False


def _is_retryable_workflow_execution_error(exc: BaseException) -> bool:
    return isinstance(exc, (TypeError, ValueError))


class CheckoutService:
    def __init__(self, client: WorkflowClient, order_service: OrderService) -> None:
        self._client = client
        self._order_service = order_service

    async def checkout(self, order_id: str) -> CheckoutWorkflowResult:
        request_start = time.perf_counter()
        if TRACE_LOGGING_ENABLED:
            logger.info("checkout.request.start order_id=%s", order_id)
        try:
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
            result = await self._execute_workflow_with_retry(
                order=order,
                workflow_id=workflow_id,
                order_id=order_id,
                attempt_no=attempt_no,
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
        except PaymentDeclinedError:
            raise
        except BaseException as exc:
            if _is_transient_checkout_backend_error(exc):
                if TRACE_LOGGING_ENABLED:
                    logger.warning(
                        "checkout.request.dependency_unavailable order_id=%s error_type=%s message=%s duration_ms=%.2f",
                        order_id,
                        exc.__class__.__name__,
                        exc,
                        elapsed_ms(request_start),
                    )
                raise UpstreamServiceError(
                    "Checkout is temporarily unavailable. Please retry.",
                ) from exc
            raise

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

    async def _execute_workflow_with_retry(
        self,
        *,
        order: object,
        workflow_id: str,
        order_id: str,
        attempt_no: int,
    ) -> CheckoutWorkflowResult:
        started_at = time.perf_counter()
        delay_seconds = _WORKFLOW_EXECUTION_RETRY_INITIAL_DELAY_SECONDS
        retry_attempt = 1
        while True:
            try:
                result: CheckoutWorkflowResult = await self._client.execute_workflow(
                    OrderCheckoutWorkflow.run,
                    order,
                    id=workflow_id,
                    task_queue="orders",
                    start_policy=StartPolicy.ATTACH_OR_START,
                )
                return result
            except BaseException as exc:
                if not _is_retryable_workflow_execution_error(exc):
                    raise
                if time.perf_counter() - started_at >= _WORKFLOW_EXECUTION_RETRY_TIMEOUT_SECONDS:
                    raise UpstreamServiceError(
                        "Checkout is temporarily unavailable. Please retry.",
                    ) from exc
                if TRACE_LOGGING_ENABLED:
                    logger.warning(
                        "checkout.request.workflow_retry order_id=%s workflow_id=%s attempt_no=%d retry_attempt=%d error_type=%s message=%s",
                        order_id,
                        workflow_id,
                        attempt_no,
                        retry_attempt,
                        exc.__class__.__name__,
                        exc,
                    )
                await asyncio.sleep(delay_seconds)
                delay_seconds = min(
                    delay_seconds * 2,
                    _WORKFLOW_EXECUTION_RETRY_MAX_DELAY_SECONDS,
                )
                retry_attempt += 1
