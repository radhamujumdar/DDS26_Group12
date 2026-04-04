"""Timer and recovery loop for the Fluxi engine."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import time

from redis.asyncio.sentinel import MasterNotFoundError
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError

from .config import FluxiSettings, create_redis_client
from .observability import elapsed_ms, trace_logging_enabled
from .store import FluxiRedisStore

_REDIS_RECOVERABLE_ERRORS = (
    ConnectionError,
    TimeoutError,
    MasterNotFoundError,
    ReadOnlyError,
)

logger = logging.getLogger(__name__)
TRACE_LOGGING_ENABLED = trace_logging_enabled()


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
        self._consumer_name = f"fluxi-scheduler:{socket.gethostname()}"

    @classmethod
    def from_settings(cls, settings: FluxiSettings | None = None) -> FluxiScheduler:
        runtime_settings = settings or FluxiSettings.from_env()
        redis = create_redis_client(runtime_settings)
        return cls(FluxiRedisStore(redis, runtime_settings), runtime_settings)

    async def run_once(self) -> dict[str, object]:
        operation_start = time.perf_counter()
        timer_member = await self.store.pop_due_timer()
        timer_result = None
        if timer_member is not None:
            timer_result = await self.store.apply_timer(timer_member)
        cleaned = await self.store.cleanup_stale_pending_entries(
            self._consumer_name
        )
        result = {
            "timer_member": timer_member,
            "timer_result": timer_result,
            "cleaned_pending_entries": cleaned,
        }
        if TRACE_LOGGING_ENABLED and (
            timer_member is not None or cleaned > 0 or elapsed_ms(operation_start) >= 50
        ):
            logger.info(
                "fluxi.scheduler.run_once timer_member=%s cleaned_pending_entries=%d duration_ms=%.2f",
                timer_member,
                cleaned,
                elapsed_ms(operation_start),
            )
        return result

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
