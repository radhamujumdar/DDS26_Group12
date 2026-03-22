"""Public workflow decorator contract and workflow-only APIs."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import inspect
from typing import Any, Callable, TypeVar, overload

from .errors import InvalidWorkflowDefinitionError, WorkflowContextUnavailableError
from .types import RetryPolicy

_WORKFLOW_DEFINITION_ATTR = "__fluxi_workflow_definition__"
_WORKFLOW_RUN_ATTR = "__fluxi_workflow_run__"

WorkflowClassT = TypeVar("WorkflowClassT")


@dataclass(frozen=True, slots=True)
class _WorkflowDefinition:
    name: str
    run_method_name: str
    default_task_queue: str | None = None


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


async def execute_activity(
    name: str,
    *,
    args: Sequence[Any] = (),
    task_queue: str | None = None,
    retry_policy: RetryPolicy | None = None,
    timeout_seconds: float | None = None,
) -> Any:
    """Schedule an activity from inside workflow code."""

    _ = (name, args, task_queue, retry_policy, timeout_seconds)
    raise WorkflowContextUnavailableError(
        "workflow.execute_activity() requires an active workflow execution context."
    )


__all__ = ["defn", "execute_activity", "run"]
