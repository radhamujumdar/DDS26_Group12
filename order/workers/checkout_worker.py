from __future__ import annotations

import asyncio

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
    )
    try:
        await worker.run()
    finally:
        await close_order_worker_container(container)


if __name__ == "__main__":
    asyncio.run(main())
