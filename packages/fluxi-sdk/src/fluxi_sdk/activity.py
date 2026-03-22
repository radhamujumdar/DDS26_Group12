"""Public activity decorator contract."""

from __future__ import annotations

from dataclasses import dataclass
import inspect
from typing import Any, Callable, TypeVar, overload

from .errors import (
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


__all__ = ["ActivityRegistration", "ActivityRegistry", "defn"]
