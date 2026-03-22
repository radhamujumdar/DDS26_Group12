"""Public activity decorator contract."""

from __future__ import annotations

from dataclasses import dataclass
import inspect
from typing import Any, Callable, TypeVar, overload

from .errors import InvalidActivityDefinitionError

_ACTIVITY_DEFINITION_ATTR = "__fluxi_activity_definition__"

ActivityCallableT = TypeVar("ActivityCallableT", bound=Callable[..., Any])


@dataclass(frozen=True, slots=True)
class _ActivityDefinition:
    name: str


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

        definition = _ActivityDefinition(name=name or fn.__name__)
        setattr(fn, _ACTIVITY_DEFINITION_ATTR, definition)
        return fn

    if target is None:
        return decorator
    return decorator(target)


__all__ = ["defn"]
