from __future__ import annotations

import asyncio

from fluxi_sdk.client import EngineConnectionConfig, WorkflowClient
from fluxi_sdk.worker import Worker

from stock.app.dependencies import build_stock_container, close_stock_container
from stock.app.services.stock_service import create_stock_activities


async def main() -> None:
    container = await build_stock_container()
    engine = EngineConnectionConfig.from_env()
    client = WorkflowClient.connect(engine=engine)
    activities = create_stock_activities(container.stock_service)
    worker = Worker(
        client,
        task_queue="stock",
        activities=activities,
    )
    try:
        await worker.run()
    finally:
        await close_stock_container(container)


if __name__ == "__main__":
    asyncio.run(main())
