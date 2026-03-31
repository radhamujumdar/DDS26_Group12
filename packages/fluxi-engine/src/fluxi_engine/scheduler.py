"""Timer and recovery loop for the Fluxi engine."""

from __future__ import annotations

import asyncio
import contextlib
import logging

from redis.asyncio.sentinel import MasterNotFoundError
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError

from .config import FluxiSettings, create_redis_client
from .store import FluxiRedisStore

_REDIS_RECOVERABLE_ERRORS = (
    ConnectionError,
    TimeoutError,
    MasterNotFoundError,
    ReadOnlyError,
)

logger = logging.getLogger(__name__)


class FluxiScheduler:
    """Owns timer expiry handling and stale PEL cleanup."""

    def __init__(
        self,
        store: FluxiRedisStore,
        settings: FluxiSettings,
    ) -> None:
        self.store = store
        self.settings = settings
        self._stopped = asyncio.Event()

    @classmethod
    def from_settings(cls, settings: FluxiSettings | None = None) -> FluxiScheduler:
        runtime_settings = settings or FluxiSettings.from_env()
        redis = create_redis_client(runtime_settings)
        return cls(FluxiRedisStore(redis, runtime_settings), runtime_settings)

    async def run_once(self) -> dict[str, object]:
        timer_member = await self.store.pop_due_timer()
        timer_result = None
        if timer_member is not None:
            timer_result = await self.store.apply_timer(timer_member)
        cleaned = await self.store.cleanup_stale_pending_entries()
        return {
            "timer_member": timer_member,
            "timer_result": timer_result,
            "cleaned_pending_entries": cleaned,
        }

    async def run_forever(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except _REDIS_RECOVERABLE_ERRORS as exc:
                logger.warning(
                    "Transient Redis/Sentinel error in FluxiScheduler loop: %s",
                    exc,
                )
            except Exception:
                logger.exception("Unexpected error in FluxiScheduler loop.")
            await asyncio.sleep(self.settings.timer_poll_interval_ms / 1000)

    async def stop(self) -> None:
        self._stopped.set()

    async def aclose(self) -> None:
        await self.stop()
        await self.store.aclose()

    async def __aenter__(self) -> FluxiScheduler:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with contextlib.suppress(Exception):
            await self.aclose()
