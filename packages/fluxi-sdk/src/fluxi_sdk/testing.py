"""In-memory runtime for exercising the SDK without external services."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import inspect
from typing import Any

from . import activity, workflow
from .activity import ActivityRegistration
from .client import WorkflowClient
from .errors import WorkflowAlreadyStartedError
from .types import ActivityOptions, StartPolicy, WorkflowReference
from .workflow import WorkflowRegistration


@dataclass(slots=True)
class ActivityExecutionRecord:
    """Recorded metadata and outcome for one activity execution."""

    sequence: int
    activity_name: str
    args: tuple[Any, ...]
    options: ActivityOptions
    status: str = "running"
    result: Any = None
    error: BaseException | None = None


@dataclass(slots=True)
class WorkflowExecutionRecord:
    """Recorded metadata and outcome for one workflow run."""

    run_id: str
    workflow_name: str
    workflow_key: str
    args: tuple[Any, ...]
    start_policy: StartPolicy
    default_task_queue: str | None
    status: str = "running"
    result: Any = None
    error: BaseException | None = None
    attach_count: int = 0
    activity_executions: list[ActivityExecutionRecord] = field(default_factory=list)


@dataclass(slots=True)
class _WorkflowRunHandle:
    record: WorkflowExecutionRecord
    task: asyncio.Task[Any]


class _FakeWorkflowClient(WorkflowClient):
    def __init__(self, runtime: FakeFluxiRuntime) -> None:
        self._runtime = runtime

    async def execute_workflow(
        self,
        workflow: WorkflowReference,
        *,
        workflow_key: str,
        args: tuple[Any, ...] = (),
        start_policy: StartPolicy = StartPolicy.ATTACH_OR_START,
    ) -> Any:
        return await self._runtime._execute_workflow(
            workflow,
            workflow_key=workflow_key,
            args=tuple(args),
            start_policy=start_policy,
        )


class FakeFluxiRuntime:
    """Single-process in-memory runtime for SDK tests and local development."""

    def __init__(self) -> None:
        self.workflow_registry = workflow.WorkflowRegistry()
        self.activity_registry = activity.ActivityRegistry()
        self._client = _FakeWorkflowClient(self)
        self._workflow_runs: list[WorkflowExecutionRecord] = []
        self._workflow_runs_by_id: dict[str, WorkflowExecutionRecord] = {}
        self._workflow_runs_by_key: dict[str, list[WorkflowExecutionRecord]] = {}
        self._latest_handle_by_key: dict[str, _WorkflowRunHandle] = {}
        self._next_run_number = 1
        self._state_lock = asyncio.Lock()

    def create_client(self) -> WorkflowClient:
        return self._client

    def register_workflow(
        self,
        workflow_cls: type[Any],
    ) -> WorkflowRegistration:
        return self.workflow_registry.register(workflow_cls)

    def register_activity(
        self,
        fn: Any,
    ) -> ActivityRegistration:
        return self.activity_registry.register(fn)

    @property
    def workflow_runs(self) -> tuple[WorkflowExecutionRecord, ...]:
        return tuple(self._workflow_runs)

    def get_workflow_run(self, run_id: str) -> WorkflowExecutionRecord:
        return self._workflow_runs_by_id[run_id]

    def get_workflow_runs_for_key(
        self,
        workflow_key: str,
    ) -> tuple[WorkflowExecutionRecord, ...]:
        return tuple(self._workflow_runs_by_key.get(workflow_key, ()))

    async def _execute_workflow(
        self,
        workflow_ref: WorkflowReference,
        *,
        workflow_key: str,
        args: tuple[Any, ...],
        start_policy: StartPolicy,
    ) -> Any:
        async with self._state_lock:
            existing_handle = self._latest_handle_by_key.get(workflow_key)
            if existing_handle is not None:
                if start_policy is StartPolicy.REJECT_DUPLICATE:
                    raise WorkflowAlreadyStartedError(
                        f"Workflow key {workflow_key!r} is already in use."
                    )
                if start_policy is StartPolicy.ATTACH_OR_START:
                    existing_handle.record.attach_count += 1
                    handle = existing_handle
                else:
                    handle = self._start_workflow_run(
                        workflow_ref,
                        workflow_key=workflow_key,
                        args=args,
                        start_policy=start_policy,
                    )
            else:
                handle = self._start_workflow_run(
                    workflow_ref,
                    workflow_key=workflow_key,
                    args=args,
                    start_policy=start_policy,
                )

        return await handle.task

    def _start_workflow_run(
        self,
        workflow_ref: WorkflowReference,
        *,
        workflow_key: str,
        args: tuple[Any, ...],
        start_policy: StartPolicy,
    ) -> _WorkflowRunHandle:
        registration = self.workflow_registry.get(workflow_ref)
        run_id = f"run-{self._next_run_number}"
        self._next_run_number += 1

        record = WorkflowExecutionRecord(
            run_id=run_id,
            workflow_name=registration.name,
            workflow_key=workflow_key,
            args=args,
            start_policy=start_policy,
            default_task_queue=registration.default_task_queue,
        )
        self._workflow_runs.append(record)
        self._workflow_runs_by_id[record.run_id] = record
        self._workflow_runs_by_key.setdefault(workflow_key, []).append(record)

        handle = _WorkflowRunHandle(
            record=record,
            task=asyncio.create_task(
                self._run_workflow(registration, record),
                name=f"fluxi:{registration.name}:{workflow_key}:{run_id}",
            ),
        )
        self._latest_handle_by_key[workflow_key] = handle
        return handle

    async def _run_workflow(
        self,
        registration: WorkflowRegistration,
        record: WorkflowExecutionRecord,
    ) -> Any:
        workflow_instance = registration.workflow()
        run_method = getattr(workflow_instance, registration.run_method_name)

        try:
            with workflow._activate_execution_context(
                registration,
                lambda name, args, options: self._execute_activity(
                    record,
                    name,
                    args,
                    options,
                ),
            ):
                result = await run_method(*record.args)
        except BaseException as exc:
            record.status = "failed"
            record.error = exc
            raise

        record.status = "completed"
        record.result = result
        return result

    async def _execute_activity(
        self,
        workflow_record: WorkflowExecutionRecord,
        activity_name: str,
        args: tuple[Any, ...],
        options: ActivityOptions,
    ) -> Any:
        execution = ActivityExecutionRecord(
            sequence=len(workflow_record.activity_executions) + 1,
            activity_name=activity_name,
            args=args,
            options=options,
        )
        workflow_record.activity_executions.append(execution)

        try:
            registration = self.activity_registry.get(activity_name)
            result = registration.fn(*args)
            if inspect.isawaitable(result):
                result = await result
        except BaseException as exc:
            execution.status = "failed"
            execution.error = exc
            raise

        execution.status = "completed"
        execution.result = result
        return result


__all__ = [
    "ActivityExecutionRecord",
    "FakeFluxiRuntime",
    "WorkflowExecutionRecord",
]
