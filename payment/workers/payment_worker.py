from __future__ import annotations

import asyncio
import os

from fluxi_sdk.client import EngineConnectionConfig, WorkflowClient
from fluxi_sdk.worker import Worker
from fluxi_engine.observability import configure_logging

from payment.app.dependencies import (
    build_payment_container,
    close_payment_container,
)
from payment.app.services.payment_service import create_payment_activities


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


async def main() -> None:
    configure_logging("payment-activity-worker")
    container = await build_payment_container()
    engine = EngineConnectionConfig.from_env()
    client = WorkflowClient.connect(engine=engine)
    activities = create_payment_activities(container.payment_service)
    worker = Worker(
        client,
        task_queue="payment",
        activities=activities,
        max_concurrent_activity_tasks=_env_int(
            "FLUXI_MAX_CONCURRENT_ACTIVITY_TASKS",
            16,
        ),
    )
    try:
        await worker.run()
    finally:
        await close_payment_container(container)


if __name__ == "__main__":
    asyncio.run(main())
