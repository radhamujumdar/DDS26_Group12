from __future__ import annotations

from fluxi_sdk.client import WorkflowClient
from fluxi_sdk.types import StartPolicy
from shop_common.checkout import (
    CheckoutWorkflowResult,
    PaymentDeclinedError,
    StockUnavailableError,
)

from ..workflows.checkout import OrderCheckoutWorkflow


class CheckoutService:
    def __init__(self, client: WorkflowClient) -> None:
        self._client = client

    async def checkout(self, order_id: str) -> CheckoutWorkflowResult:
        result: CheckoutWorkflowResult = await self._client.execute_workflow(
            OrderCheckoutWorkflow.run,
            order_id,
            id=f"order-checkout:{order_id}",
            task_queue="orders",
            start_policy=StartPolicy.ALLOW_DUPLICATE,
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
