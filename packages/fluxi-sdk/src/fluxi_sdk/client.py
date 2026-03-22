"""Public workflow client contract."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Callable, Protocol

from .types import StartPolicy, WorkflowReference


class _WorkflowClientBackend(Protocol):
    async def _execute_workflow(
        self,
        workflow: WorkflowReference,
        *,
        workflow_id: str,
        task_queue: str,
        args: tuple[Any, ...],
        start_policy: StartPolicy,
    ) -> Any: ...

    def _create_worker_binding(
        self,
        *,
        task_queue: str,
        workflows: Sequence[type[Any]],
        activities: Sequence[Callable[..., Any]],
    ) -> Any: ...

    def _activate_worker_binding(self, binding: Any) -> None: ...

    def _deactivate_worker_binding(self, binding: Any) -> None: ...


class WorkflowClient:
    """Concrete client facade backed by the phase-1 fake runtime."""

    def __init__(self, backend: _WorkflowClientBackend) -> None:
        self._backend = backend

    @classmethod
    def connect(cls, runtime: _WorkflowClientBackend | None = None) -> WorkflowClient:
        if runtime is None:
            from .testing import FakeFluxiRuntime

            runtime = FakeFluxiRuntime()
        return cls(runtime)

    async def execute_workflow(
        self,
        workflow: WorkflowReference,
        *workflow_args: Any,
        id: str | None = None,
        task_queue: str | None = None,
        start_policy: StartPolicy = StartPolicy.ATTACH_OR_START,
        workflow_key: str | None = None,
        args: Sequence[Any] | None = None,
    ) -> Any:
        """Execute or attach to a workflow using the provided start policy."""

        workflow_id = _coalesce_workflow_id(id, workflow_key)
        if task_queue is None:
            raise TypeError(
                "execute_workflow() missing required keyword argument: 'task_queue'"
            )

        normalized_args = _normalize_workflow_args(workflow_args, args)
        return await self._backend._execute_workflow(
            workflow,
            workflow_id=workflow_id,
            task_queue=task_queue,
            args=normalized_args,
            start_policy=start_policy,
        )

    def _create_worker_binding(
        self,
        *,
        task_queue: str,
        workflows: Sequence[type[Any]],
        activities: Sequence[Callable[..., Any]],
    ) -> Any:
        return self._backend._create_worker_binding(
            task_queue=task_queue,
            workflows=workflows,
            activities=activities,
        )

    def _activate_worker_binding(self, binding: Any) -> None:
        self._backend._activate_worker_binding(binding)

    def _deactivate_worker_binding(self, binding: Any) -> None:
        self._backend._deactivate_worker_binding(binding)


def _coalesce_workflow_id(id: str | None, workflow_key: str | None) -> str:
    if id is not None and workflow_key is not None and id != workflow_key:
        raise ValueError("Use either id or workflow_key, not both with different values.")
    workflow_id = id or workflow_key
    if workflow_id is None:
        raise TypeError("execute_workflow() missing required keyword argument: 'id'")
    if not workflow_id.strip():
        raise ValueError("Workflow id must be a non-empty string.")
    return workflow_id


def _normalize_workflow_args(
    workflow_args: tuple[Any, ...],
    args: Sequence[Any] | None,
) -> tuple[Any, ...]:
    if args is not None and workflow_args:
        raise ValueError("Use positional workflow args or args=, not both.")
    if args is not None:
        return tuple(args)
    return workflow_args


__all__ = ["WorkflowClient"]
