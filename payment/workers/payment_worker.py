from __future__ import annotations

import asyncio

from fluxi_sdk.client import EngineConnectionConfig, WorkflowClient
from fluxi_sdk.worker import Worker

from payment.app.dependencies import (
    build_payment_container,
    close_payment_container,
)
from payment.app.services.payment_service import create_payment_activities


async def main() -> None:
    container = await build_payment_container()
    engine = EngineConnectionConfig.from_env()
    client = WorkflowClient.connect(engine=engine)
    activities = create_payment_activities(container.payment_service)
    worker = Worker(
        client,
        task_queue="payment",
        activities=activities,
    )
    try:
        await worker.run()
    finally:
        await close_payment_container(container)


if __name__ == "__main__":
    asyncio.run(main())
