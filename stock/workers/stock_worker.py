from __future__ import annotations

import asyncio
import os

from fluxi_sdk.client import EngineConnectionConfig, WorkflowClient
from fluxi_sdk.worker import Worker
from fluxi_engine.observability import configure_logging

from stock.app.dependencies import build_stock_container, close_stock_container
from stock.app.services.stock_service import create_stock_activities


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


async def main() -> None:
    configure_logging("stock-activity-worker")
    container = await build_stock_container()
    engine = EngineConnectionConfig.from_env()
    client = WorkflowClient.connect(engine=engine)
    activities = create_stock_activities(container.stock_service)
    worker = Worker(
        client,
        task_queue="stock",
        activities=activities,
        max_concurrent_activity_tasks=_env_int(
            "FLUXI_MAX_CONCURRENT_ACTIVITY_TASKS",
            16,
        ),
    )
    try:
        await worker.run()
    finally:
        await close_stock_container(container)


if __name__ == "__main__":
    asyncio.run(main())
