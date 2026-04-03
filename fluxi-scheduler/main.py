import asyncio

from fluxi_engine.config import FluxiSettings
from fluxi_engine.observability import configure_logging
from fluxi_engine.scheduler import FluxiScheduler


async def _main() -> None:
    settings = FluxiSettings.from_env()
    scheduler = FluxiScheduler.from_settings(settings)
    try:
        await scheduler.run_forever()
    finally:
        await scheduler.aclose()


if __name__ == "__main__":
    configure_logging("fluxi-scheduler")
    asyncio.run(_main())
