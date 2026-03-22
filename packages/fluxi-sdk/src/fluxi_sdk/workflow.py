"""Public workflow decorator contract and workflow-only APIs."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import inspect
from typing import Any, TypeVar, overload

from .errors import (
    DuplicateWorkflowRegistrationError,
    InvalidWorkflowDefinitionError,
    UnknownWorkflowError,
    WorkflowContextUnavailableError,
)
from .types import ActivityOptions, RetryPolicy, WorkflowReference

_WORKFLOW_DEFINITION_ATTR = "__fluxi_workflow_definition__"
_WORKFLOW_RUN_ATTR = "__fluxi_workflow_run__"

WorkflowClassT = TypeVar("WorkflowClassT")


@dataclass(frozen=True, slots=True)
class _WorkflowDefinition:
    name: str
    run_method_name: str
    default_task_queue: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowRegistration:
    """A registered workflow entry."""

    name: str
    workflow: type[Any]
    run_method_name: str
    default_task_queue: str | None = None


@dataclass(frozen=True, slots=True)
class _WorkflowExecutionContext:
    registration: WorkflowRegistration
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
    default_task_queue: str | None = None,
) -> Callable[[type[WorkflowClassT]], type[WorkflowClassT]]: ...


def defn(
    target: type[WorkflowClassT] | None = None,
    *,
    name: str | None = None,
    default_task_queue: str | None = None,
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

        if default_task_queue is not None and not default_task_queue.strip():
            raise InvalidWorkflowDefinitionError(
                "default_task_queue must be a non-empty string when provided."
            )

        definition = _WorkflowDefinition(
            name=name or cls.__name__,
            run_method_name=run_method_names[0],
            default_task_queue=default_task_queue,
        )
        setattr(cls, _WORKFLOW_DEFINITION_ATTR, definition)
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
            default_task_queue=definition.default_task_queue,
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
    activity_executor: Callable[
        [str, Sequence[Any], ActivityOptions],
        Awaitable[Any],
    ],
) -> Iterator[None]:
    context = _WorkflowExecutionContext(
        registration=registration,
        activity_executor=activity_executor,
    )
    token = _CURRENT_WORKFLOW_CONTEXT.set(context)
    try:
        yield
    finally:
        _CURRENT_WORKFLOW_CONTEXT.reset(token)


async def execute_activity(
    name: str,
    *,
    args: Sequence[Any] = (),
    task_queue: str | None = None,
    retry_policy: RetryPolicy | None = None,
    timeout_seconds: float | None = None,
) -> Any:
    """Schedule an activity from inside workflow code."""

    context = _CURRENT_WORKFLOW_CONTEXT.get()
    if context is None:
        raise WorkflowContextUnavailableError(
            "workflow.execute_activity() requires an active workflow execution context."
        )

    options = ActivityOptions(
        task_queue=task_queue or context.registration.default_task_queue,
        retry_policy=retry_policy,
        timeout_seconds=timeout_seconds,
    )
    return await context.activity_executor(name, tuple(args), options)


__all__ = ["WorkflowRegistration", "WorkflowRegistry", "defn", "execute_activity", "run"]
