"""Public worker facade aligned with Temporal's developer API shape."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from .client import WorkflowClient


class Worker:
    """Phase-1 in-process worker that owns registration and queue binding."""

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

    async def run(self) -> None:
        if self._running:
            return
        self._client._activate_worker_binding(self._binding)
        self._running = True

    async def shutdown(self) -> None:
        if not self._running:
            return
        self._client._deactivate_worker_binding(self._binding)
        self._running = False

    async def __aenter__(self) -> Worker:
        await self.run()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()


__all__ = ["Worker"]
