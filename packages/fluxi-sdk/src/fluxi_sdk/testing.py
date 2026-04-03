"""In-memory runtime for exercising the SDK without external services."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import inspect
from typing import Any

from . import activity, workflow
from .activity import ActivityRegistration
from .client import WorkflowClient
from .errors import (
    DuplicateActivityRegistrationError,
    DuplicateWorkflowRegistrationError,
    NoActivityWorkerAvailableError,
    NoWorkflowWorkerAvailableError,
    WorkflowAlreadyStartedError,
)
from .types import ActivityOptions, StartPolicy, WorkflowReference
from .workflow import WorkflowRegistration


@dataclass(slots=True)
class ActivityExecutionRecord:
    """Recorded metadata and outcome for one activity execution."""

    sequence: int
    activity_execution_id: str
    activity_name: str
    workflow_id: str
    run_id: str
    task_queue: str
    attempt_no: int
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
    workflow_id: str
    task_queue: str
    args: tuple[Any, ...]
    start_policy: StartPolicy
    status: str = "running"
    result: Any = None
    error: BaseException | None = None
    attach_count: int = 0
    activity_executions: list[ActivityExecutionRecord] = field(default_factory=list)

    @property
    def workflow_key(self) -> str:
        return self.workflow_id


@dataclass(slots=True)
class _WorkflowRunHandle:
    record: WorkflowExecutionRecord
    task: asyncio.Task[Any]


class _FakeActivityHandle(workflow.ActivityHandle[Any]):
    def __init__(self, task: asyncio.Task[Any]) -> None:
        self.task = task
        super().__init__(lambda: self.task)

@dataclass(slots=True)
class _WorkerBinding:
    task_queue: str
    workflows: tuple[WorkflowRegistration, ...]
    activities: tuple[ActivityRegistration, ...]
    running: bool = False
    _stopped: asyncio.Event = field(default_factory=asyncio.Event)

    async def start(self) -> None:
        if self.running:
            return
        if self._stopped.is_set():
            self._stopped = asyncio.Event()
        self.running = True

    async def wait(self) -> None:
        await self._stopped.wait()

    async def shutdown(self) -> None:
        if not self.running:
            return
        self.running = False
        self._stopped.set()


class FakeFluxiRuntime:
    """Single-process in-memory runtime for SDK tests and local development."""

    def __init__(self) -> None:
        self.workflow_registry = workflow.WorkflowRegistry()
        self.activity_registry = activity.ActivityRegistry()
        self._client = WorkflowClient(self)
        self._worker_bindings: list[_WorkerBinding] = []
        self._workflow_runs: list[WorkflowExecutionRecord] = []
        self._workflow_runs_by_run_id: dict[str, WorkflowExecutionRecord] = {}
        self._workflow_runs_by_workflow_id: dict[str, list[WorkflowExecutionRecord]] = {}
        self._latest_handle_by_workflow_id: dict[str, _WorkflowRunHandle] = {}
        self._next_run_number = 1
        self._state_lock = asyncio.Lock()

    def create_client(self) -> WorkflowClient:
        return self._client

    async def aclose(self) -> None:
        return

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
        return self._workflow_runs_by_run_id[run_id]

    def get_workflow_runs_for_id(
        self,
        workflow_id: str,
    ) -> tuple[WorkflowExecutionRecord, ...]:
        return tuple(self._workflow_runs_by_workflow_id.get(workflow_id, ()))

    def get_workflow_runs_for_key(
        self,
        workflow_key: str,
    ) -> tuple[WorkflowExecutionRecord, ...]:
        return self.get_workflow_runs_for_id(workflow_key)

    async def _execute_workflow(
        self,
        workflow_ref: WorkflowReference,
        *,
        workflow_id: str,
        task_queue: str,
        args: tuple[Any, ...],
        start_policy: StartPolicy,
    ) -> Any:
        registration = self._get_workflow_registration_for_task_queue(
            workflow_ref,
            task_queue=task_queue,
        )

        async with self._state_lock:
            existing_handle = self._latest_handle_by_workflow_id.get(workflow_id)
            if existing_handle is not None:
                if start_policy is StartPolicy.REJECT_DUPLICATE:
                    raise WorkflowAlreadyStartedError(
                        f"Workflow id {workflow_id!r} is already in use."
                    )
                if start_policy is StartPolicy.ATTACH_OR_START:
                    existing_handle.record.attach_count += 1
                    handle = existing_handle
                else:
                    handle = self._start_workflow_run(
                        registration,
                        workflow_id=workflow_id,
                        task_queue=task_queue,
                        args=args,
                        start_policy=start_policy,
                    )
            else:
                handle = self._start_workflow_run(
                    registration,
                    workflow_id=workflow_id,
                    task_queue=task_queue,
                    args=args,
                    start_policy=start_policy,
                )

        return await handle.task

    def _start_workflow_run(
        self,
        registration: WorkflowRegistration,
        *,
        workflow_id: str,
        task_queue: str,
        args: tuple[Any, ...],
        start_policy: StartPolicy,
    ) -> _WorkflowRunHandle:
        run_id = f"run-{self._next_run_number}"
        self._next_run_number += 1

        record = WorkflowExecutionRecord(
            run_id=run_id,
            workflow_name=registration.name,
            workflow_id=workflow_id,
            task_queue=task_queue,
            args=args,
            start_policy=start_policy,
        )
        self._workflow_runs.append(record)
        self._workflow_runs_by_run_id[record.run_id] = record
        self._workflow_runs_by_workflow_id.setdefault(workflow_id, []).append(record)

        handle = _WorkflowRunHandle(
            record=record,
            task=asyncio.create_task(
                self._run_workflow(registration, record),
                name=f"fluxi:{registration.name}:{workflow_id}:{run_id}",
            ),
        )
        self._latest_handle_by_workflow_id[workflow_id] = handle
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
                record.task_queue,
                lambda name, args, options: self._start_activity(
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

    def _start_activity(
        self,
        workflow_record: WorkflowExecutionRecord,
        activity_name: str,
        args: tuple[Any, ...],
        options: ActivityOptions,
    ) -> workflow.ActivityHandle[Any]:
        activity_execution_id = (
            f"{workflow_record.run_id}:act:{len(workflow_record.activity_executions) + 1}"
        )
        task_queue = options.task_queue or workflow_record.task_queue
        execution = ActivityExecutionRecord(
            sequence=len(workflow_record.activity_executions) + 1,
            activity_execution_id=activity_execution_id,
            activity_name=activity_name,
            workflow_id=workflow_record.workflow_id,
            run_id=workflow_record.run_id,
            task_queue=task_queue,
            attempt_no=1,
            args=args,
            options=options,
        )
        workflow_record.activity_executions.append(execution)
        task = asyncio.create_task(
            self._run_activity_execution(
                workflow_record=workflow_record,
                execution=execution,
            ),
            name=(
                f"fluxi:activity:{activity_name}:"
                f"{workflow_record.workflow_id}:{activity_execution_id}"
            ),
        )
        return _FakeActivityHandle(task)

    async def _run_activity_execution(
        self,
        *,
        workflow_record: WorkflowExecutionRecord,
        execution: ActivityExecutionRecord,
    ) -> Any:
        try:
            registration = self._get_activity_registration_for_task_queue(
                execution.activity_name,
                task_queue=execution.task_queue,
            )
            with activity._activate_execution_context(
                activity.ActivityExecutionInfo(
                    activity_execution_id=execution.activity_execution_id,
                    attempt_no=execution.attempt_no,
                    activity_name=execution.activity_name,
                    task_queue=execution.task_queue,
                    workflow_id=workflow_record.workflow_id,
                    run_id=workflow_record.run_id,
                )
            ):
                result = registration.fn(*execution.args)
                if inspect.isawaitable(result):
                    result = await result
        except BaseException as exc:
            execution.status = "failed"
            execution.error = exc
            raise

        execution.status = "completed"
        execution.result = result
        return result

    def _create_worker_binding(
        self,
        *,
        task_queue: str,
        workflows: tuple[type[Any], ...] | list[type[Any]],
        activities: tuple[Any, ...] | list[Any],
        max_concurrent_workflow_tasks: int,
        max_concurrent_activity_tasks: int,
    ) -> _WorkerBinding:
        workflow_registrations = tuple(
            self._ensure_workflow_registered(workflow_ref)
            for workflow_ref in workflows
        )
        activity_registrations = tuple(
            self._ensure_activity_registered(activity_ref)
            for activity_ref in activities
        )

        binding = _WorkerBinding(
            task_queue=task_queue,
            workflows=workflow_registrations,
            activities=activity_registrations,
        )
        self._worker_bindings.append(binding)
        return binding

    def _ensure_workflow_registered(
        self,
        workflow_ref: type[Any],
    ) -> WorkflowRegistration:
        try:
            return self.workflow_registry.register(workflow_ref)
        except DuplicateWorkflowRegistrationError:
            return self.workflow_registry.get(workflow_ref)

    def _ensure_activity_registered(
        self,
        activity_ref: Any,
    ) -> ActivityRegistration:
        try:
            return self.activity_registry.register(activity_ref)
        except DuplicateActivityRegistrationError:
            return self.activity_registry.get(activity_ref)

    def _get_workflow_registration_for_task_queue(
        self,
        workflow_ref: WorkflowReference,
        *,
        task_queue: str,
    ) -> WorkflowRegistration:
        registration = self.workflow_registry.get(workflow_ref)
        for binding in self._worker_bindings:
            if (
                binding.running
                and binding.task_queue == task_queue
                and registration in binding.workflows
            ):
                return registration
        raise NoWorkflowWorkerAvailableError(
            f"No running worker for task_queue {task_queue!r} can execute workflow {registration.name!r}."
        )

    def _get_activity_registration_for_task_queue(
        self,
        activity_ref: str,
        *,
        task_queue: str | None,
    ) -> ActivityRegistration:
        registration = self.activity_registry.get(activity_ref)
        if task_queue is None:
            raise NoActivityWorkerAvailableError(
                f"Activity {registration.name!r} was scheduled without a task queue."
            )
        for binding in self._worker_bindings:
            if (
                binding.running
                and binding.task_queue == task_queue
                and registration in binding.activities
            ):
                return registration
        raise NoActivityWorkerAvailableError(
            f"No running worker for task_queue {task_queue!r} can execute activity {registration.name!r}."
        )


__all__ = [
    "ActivityExecutionRecord",
    "FakeFluxiRuntime",
    "WorkflowExecutionRecord",
]
