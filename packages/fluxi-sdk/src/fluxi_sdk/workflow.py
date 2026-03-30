"""Public workflow decorator contract and workflow-only APIs."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
import inspect
from typing import Any, TypeVar, overload

from .activity import _get_activity_name
from .errors import (
    DuplicateWorkflowRegistrationError,
    InvalidWorkflowDefinitionError,
    UnknownWorkflowError,
    WorkflowContextUnavailableError,
)
from .types import ActivityOptions, ActivityReference, RetryPolicy, WorkflowReference

_WORKFLOW_DEFINITION_ATTR = "__fluxi_workflow_definition__"
_WORKFLOW_RUN_ATTR = "__fluxi_workflow_run__"
_WORKFLOW_CLASS_ATTR = "__fluxi_workflow_class__"

WorkflowClassT = TypeVar("WorkflowClassT")


@dataclass(frozen=True, slots=True)
class _WorkflowDefinition:
    name: str
    run_method_name: str


@dataclass(frozen=True, slots=True)
class WorkflowRegistration:
    """A registered workflow entry."""

    name: str
    workflow: type[Any]
    run_method_name: str


@dataclass(frozen=True, slots=True)
class _WorkflowExecutionContext:
    registration: WorkflowRegistration
    task_queue: str
    activity_executor: Callable[
        [str, Sequence[Any], ActivityOptions],
        Awaitable[Any],
    ]


_CURRENT_WORKFLOW_CONTEXT: ContextVar[_WorkflowExecutionContext | None] = ContextVar(
    "fluxi_current_workflow_context",
    default=None,
)


def run(method: Callable[..., Any]) -> Callable[..., Any]:
    """Mark the async workflow entrypoint method."""

    if not inspect.iscoroutinefunction(method):
        raise InvalidWorkflowDefinitionError(
            "@workflow.run must decorate an async function."
        )

    setattr(method, _WORKFLOW_RUN_ATTR, True)
    return method


@overload
def defn(target: type[WorkflowClassT]) -> type[WorkflowClassT]: ...


@overload
def defn(
    *,
    name: str | None = None,
) -> Callable[[type[WorkflowClassT]], type[WorkflowClassT]]: ...


def defn(
    target: type[WorkflowClassT] | None = None,
    *,
    name: str | None = None,
) -> (
    type[WorkflowClassT]
    | Callable[[type[WorkflowClassT]], type[WorkflowClassT]]
):
    """Declare a workflow class and attach stable definition metadata."""

    def decorator(cls: type[WorkflowClassT]) -> type[WorkflowClassT]:
        if not inspect.isclass(cls):
            raise InvalidWorkflowDefinitionError(
                "@workflow.defn must decorate a class."
            )
        if name is not None and not name.strip():
            raise InvalidWorkflowDefinitionError(
                "Workflow names must be non-empty when provided."
            )

        run_method_names = [
            attr_name
            for attr_name, attr_value in cls.__dict__.items()
            if getattr(attr_value, _WORKFLOW_RUN_ATTR, False)
        ]

        if len(run_method_names) != 1:
            raise InvalidWorkflowDefinitionError(
                "Workflow classes must define exactly one @workflow.run method."
            )

        definition = _WorkflowDefinition(
            name=name or cls.__name__,
            run_method_name=run_method_names[0],
        )
        setattr(cls, _WORKFLOW_DEFINITION_ATTR, definition)
        run_method = cls.__dict__[run_method_names[0]]
        setattr(run_method, _WORKFLOW_CLASS_ATTR, cls)
        return cls

    if target is None:
        return decorator
    return decorator(target)


def _get_workflow_definition(workflow_cls: type[Any]) -> _WorkflowDefinition:
    definition = getattr(workflow_cls, _WORKFLOW_DEFINITION_ATTR, None)
    if not isinstance(definition, _WorkflowDefinition):
        raise InvalidWorkflowDefinitionError(
            "Workflow classes must be decorated with @workflow.defn before registration."
        )
    return definition


def _get_workflow_name(workflow_ref: WorkflowReference) -> str:
    if isinstance(workflow_ref, str):
        return workflow_ref
    if inspect.isclass(workflow_ref):
        return _get_workflow_definition(workflow_ref).name
    if callable(workflow_ref):
        workflow_cls = getattr(workflow_ref, _WORKFLOW_CLASS_ATTR, None)
        if workflow_cls is not None:
            return _get_workflow_definition(workflow_cls).name
    raise InvalidWorkflowDefinitionError(
        f"Workflow reference {workflow_ref!r} is not a decorated workflow."
    )


def _get_workflow_run_callable(
    workflow_ref: WorkflowReference,
) -> Callable[..., Any] | None:
    if callable(workflow_ref) and not inspect.isclass(workflow_ref):
        return workflow_ref
    if inspect.isclass(workflow_ref):
        definition = _get_workflow_definition(workflow_ref)
        return getattr(workflow_ref, definition.run_method_name)
    return None


class WorkflowRegistry:
    """Explicit registry for workflow definitions."""

    def __init__(self) -> None:
        self._by_name: dict[str, WorkflowRegistration] = {}
        self._by_class: dict[type[Any], WorkflowRegistration] = {}

    def register(self, workflow_cls: type[Any]) -> WorkflowRegistration:
        definition = _get_workflow_definition(workflow_cls)

        if workflow_cls in self._by_class:
            raise DuplicateWorkflowRegistrationError(
                f"Workflow class {workflow_cls.__name__!r} is already registered."
            )
        if definition.name in self._by_name:
            raise DuplicateWorkflowRegistrationError(
                f"Workflow name {definition.name!r} is already registered."
            )

        registration = WorkflowRegistration(
            name=definition.name,
            workflow=workflow_cls,
            run_method_name=definition.run_method_name,
        )
        self._by_name[registration.name] = registration
        self._by_class[registration.workflow] = registration
        return registration

    def get(self, workflow: WorkflowReference) -> WorkflowRegistration:
        registration: WorkflowRegistration | None
        if isinstance(workflow, str):
            registration = self._by_name.get(workflow)
        elif inspect.isclass(workflow):
            registration = self._by_class.get(workflow)
        elif callable(workflow):
            workflow_cls = getattr(workflow, _WORKFLOW_CLASS_ATTR, None)
            registration = self._by_class.get(workflow_cls)
        else:
            registration = None

        if registration is None:
            raise UnknownWorkflowError(f"Workflow {workflow!r} is not registered.")
        return registration

    def __contains__(self, workflow: object) -> bool:
        try:
            self.get(workflow)  # type: ignore[arg-type]
        except UnknownWorkflowError:
            return False
        return True

    def values(self) -> tuple[WorkflowRegistration, ...]:
        return tuple(self._by_name.values())


@contextmanager
def _activate_execution_context(
    registration: WorkflowRegistration,
    task_queue: str,
    activity_executor: Callable[
        [str, Sequence[Any], ActivityOptions],
        Awaitable[Any],
    ],
) -> Iterator[None]:
    context = _WorkflowExecutionContext(
        registration=registration,
        task_queue=task_queue,
        activity_executor=activity_executor,
    )
    token = _CURRENT_WORKFLOW_CONTEXT.set(context)
    try:
        yield
    finally:
        _CURRENT_WORKFLOW_CONTEXT.reset(token)


def _coerce_schedule_to_close_timeout(
    schedule_to_close_timeout: timedelta | None,
    timeout_seconds: float | None,
) -> timedelta | None:
    if schedule_to_close_timeout is not None and timeout_seconds is not None:
        raise ValueError(
            "Use either schedule_to_close_timeout or timeout_seconds, not both."
        )
    if timeout_seconds is not None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0 when provided.")
        return timedelta(seconds=timeout_seconds)
    return schedule_to_close_timeout


def _normalize_activity_args(
    activity_args: tuple[Any, ...],
    args: Sequence[Any] | None,
) -> tuple[Any, ...]:
    if args is not None and activity_args:
        raise ValueError("Use positional activity args or args=, not both.")
    if args is not None:
        return tuple(args)
    return activity_args


async def execute_activity(
    activity: ActivityReference,
    *activity_args: Any,
    task_queue: str | None = None,
    retry_policy: RetryPolicy | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    args: Sequence[Any] | None = None,
    timeout_seconds: float | None = None,
) -> Any:
    """Schedule an activity from inside workflow code."""

    context = _CURRENT_WORKFLOW_CONTEXT.get()
    if context is None:
        raise WorkflowContextUnavailableError(
            "workflow.execute_activity() requires an active workflow execution context."
        )

    activity_name = _get_activity_name(activity)
    normalized_args = _normalize_activity_args(activity_args, args)
    options = ActivityOptions(
        task_queue=task_queue or context.task_queue,
        retry_policy=retry_policy,
        schedule_to_close_timeout=_coerce_schedule_to_close_timeout(
            schedule_to_close_timeout,
            timeout_seconds,
        ),
    )
    return await context.activity_executor(activity_name, normalized_args, options)


class unsafe:
    """Compatibility helpers for Temporal-style workflow modules."""

    @staticmethod
    @contextmanager
    def imports_passed_through() -> Iterator[None]:
        yield


__all__ = [
    "WorkflowRegistration",
    "WorkflowRegistry",
    "defn",
    "execute_activity",
    "run",
    "unsafe",
]
