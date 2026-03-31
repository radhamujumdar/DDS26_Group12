"""Public worker facade aligned with Temporal's developer API shape."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from .client import WorkflowClient


class Worker:
    """Worker facade that owns queue binding and backend lifecycle."""

    def __init__(
        self,
        client: WorkflowClient,
        *,
        task_queue: str,
        workflows: Sequence[type[Any]] | None = None,
        activities: Sequence[Callable[..., Any]] | None = None,
    ) -> None:
        if not task_queue.strip():
            raise ValueError("task_queue must be a non-empty string.")

        self._client = client
        self.task_queue = task_queue
        self.workflows = tuple(workflows or ())
        self.activities = tuple(activities or ())
        self._binding = client._create_worker_binding(
            task_queue=task_queue,
            workflows=self.workflows,
            activities=self.activities,
        )
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if self._running:
            return
        await self._binding.start()
        self._running = True

    async def run(self) -> None:
        await self.start()
        await self._binding.wait()

    async def shutdown(self) -> None:
        if not self._running:
            return
        await self._binding.shutdown()
        self._running = False

    async def __aenter__(self) -> Worker:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()


__all__ = ["Worker"]
