"""Public activity decorator contract."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import inspect
from typing import Any, Callable, Iterator, TypeVar, overload

from .errors import (
    ActivityContextUnavailableError,
    DuplicateActivityRegistrationError,
    InvalidActivityDefinitionError,
    UnknownActivityError,
)

_ACTIVITY_DEFINITION_ATTR = "__fluxi_activity_definition__"

ActivityCallableT = TypeVar("ActivityCallableT", bound=Callable[..., Any])


@dataclass(frozen=True, slots=True)
class _ActivityDefinition:
    name: str


@dataclass(frozen=True, slots=True)
class ActivityRegistration:
    """A registered activity entry."""

    name: str
    fn: Callable[..., Any]


@dataclass(frozen=True, slots=True)
class ActivityExecutionInfo:
    """Execution metadata exposed to running activities."""

    activity_execution_id: str
    attempt_no: int
    activity_name: str
    task_queue: str
    workflow_id: str
    run_id: str


_CURRENT_ACTIVITY_CONTEXT: ContextVar[ActivityExecutionInfo | None] = ContextVar(
    "fluxi_current_activity_context",
    default=None,
)


@overload
def defn(target: ActivityCallableT) -> ActivityCallableT: ...


@overload
def defn(*, name: str | None = None) -> Callable[[ActivityCallableT], ActivityCallableT]: ...


def defn(
    target: ActivityCallableT | None = None,
    *,
    name: str | None = None,
) -> ActivityCallableT | Callable[[ActivityCallableT], ActivityCallableT]:
    """Declare an activity and attach stable definition metadata."""

    def decorator(fn: ActivityCallableT) -> ActivityCallableT:
        if inspect.isclass(fn) or not callable(fn):
            raise InvalidActivityDefinitionError(
                "@activity.defn must decorate a callable function."
            )
        if name is not None and not name.strip():
            raise InvalidActivityDefinitionError(
                "Activity names must be non-empty when provided."
            )

        definition = _ActivityDefinition(name=name or fn.__name__)
        setattr(fn, _ACTIVITY_DEFINITION_ATTR, definition)
        return fn

    if target is None:
        return decorator
    return decorator(target)


def _get_activity_definition(fn: Callable[..., Any]) -> _ActivityDefinition:
    definition = getattr(fn, _ACTIVITY_DEFINITION_ATTR, None)
    if not isinstance(definition, _ActivityDefinition):
        raise InvalidActivityDefinitionError(
            "Activities must be decorated with @activity.defn before registration."
        )
    return definition


def _get_activity_name(activity: str | Callable[..., Any]) -> str:
    if isinstance(activity, str):
        return activity
    if callable(activity) and not inspect.isclass(activity):
        return _get_activity_definition(activity).name
    raise InvalidActivityDefinitionError(
        "Activities must be referenced by registered name or a callable decorated with @activity.defn."
    )


class ActivityRegistry:
    """Explicit registry for activity definitions."""

    def __init__(self) -> None:
        self._by_name: dict[str, ActivityRegistration] = {}
        self._by_callable: dict[Callable[..., Any], ActivityRegistration] = {}

    def register(self, fn: Callable[..., Any]) -> ActivityRegistration:
        definition = _get_activity_definition(fn)

        if fn in self._by_callable:
            raise DuplicateActivityRegistrationError(
                f"Activity callable {fn.__name__!r} is already registered."
            )
        if definition.name in self._by_name:
            raise DuplicateActivityRegistrationError(
                f"Activity name {definition.name!r} is already registered."
            )

        registration = ActivityRegistration(name=definition.name, fn=fn)
        self._by_name[registration.name] = registration
        self._by_callable[registration.fn] = registration
        return registration

    def get(self, activity: str | Callable[..., Any]) -> ActivityRegistration:
        registration: ActivityRegistration | None
        if isinstance(activity, str):
            registration = self._by_name.get(activity)
        elif callable(activity) and not inspect.isclass(activity):
            registration = self._by_callable.get(activity)
        else:
            registration = None

        if registration is None:
            raise UnknownActivityError(f"Activity {activity!r} is not registered.")
        return registration

    def __contains__(self, activity: object) -> bool:
        try:
            self.get(activity)  # type: ignore[arg-type]
        except UnknownActivityError:
            return False
        return True

    def values(self) -> tuple[ActivityRegistration, ...]:
        return tuple(self._by_name.values())


@contextmanager
def _activate_execution_context(info: ActivityExecutionInfo) -> Iterator[None]:
    token = _CURRENT_ACTIVITY_CONTEXT.set(info)
    try:
        yield
    finally:
        _CURRENT_ACTIVITY_CONTEXT.reset(token)


def info() -> ActivityExecutionInfo:
    """Return execution metadata for the currently running activity."""

    context = _CURRENT_ACTIVITY_CONTEXT.get()
    if context is None:
        raise ActivityContextUnavailableError(
            "activity.info() requires an active activity execution context."
        )
    return context


__all__ = [
    "ActivityExecutionInfo",
    "ActivityRegistration",
    "ActivityRegistry",
    "defn",
    "info",
]
