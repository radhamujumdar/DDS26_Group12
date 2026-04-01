from __future__ import annotations

import asyncio
import os

from fluxi_sdk.client import EngineConnectionConfig, WorkflowClient
from fluxi_sdk.worker import Worker

from order.app.dependencies import (
    build_order_worker_container,
    close_order_worker_container,
)
from order.app.workflows.checkout import (
    OrderCheckoutWorkflow,
    create_order_activities,
)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


async def main() -> None:
    container = await build_order_worker_container()
    engine = EngineConnectionConfig.from_env()
    client = WorkflowClient.connect(engine=engine)
    activities = create_order_activities(container.order_service)
    worker = Worker(
        client,
        task_queue="orders",
        workflows=[OrderCheckoutWorkflow],
        activities=activities,
        max_concurrent_workflow_tasks=_env_int(
            "FLUXI_MAX_CONCURRENT_WORKFLOW_TASKS",
            8,
        ),
        max_concurrent_activity_tasks=_env_int(
            "FLUXI_MAX_CONCURRENT_ACTIVITY_TASKS",
            4,
        ),
    )
    try:
        await worker.run()
    finally:
        await close_order_worker_container(container)


if __name__ == "__main__":
    asyncio.run(main())
