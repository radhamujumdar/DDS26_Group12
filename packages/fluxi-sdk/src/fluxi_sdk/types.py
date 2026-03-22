"""Public SDK types for workflow execution and activity scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class StartPolicy(str, Enum):
    """How the runtime should behave for an existing workflow key."""

    ATTACH_OR_START = "attach_or_start"
    ALLOW_DUPLICATE = "allow_duplicate"
    REJECT_DUPLICATE = "reject_duplicate"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Retry options recorded on scheduled activities."""

    max_attempts: int | None = None
    initial_interval_seconds: float | None = None
    backoff_coefficient: float | None = None
    max_interval_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1 when provided.")
        if (
            self.initial_interval_seconds is not None
            and self.initial_interval_seconds <= 0
        ):
            raise ValueError(
                "initial_interval_seconds must be greater than 0 when provided."
            )
        if self.backoff_coefficient is not None and self.backoff_coefficient < 1:
            raise ValueError(
                "backoff_coefficient must be at least 1 when provided."
            )
        if self.max_interval_seconds is not None and self.max_interval_seconds <= 0:
            raise ValueError(
                "max_interval_seconds must be greater than 0 when provided."
            )


@dataclass(frozen=True, slots=True)
class ActivityOptions:
    """Activity scheduling options captured at the workflow call site."""

    task_queue: str | None = None
    retry_policy: RetryPolicy | None = None
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.task_queue is not None and not self.task_queue.strip():
            raise ValueError("task_queue must be a non-empty string when provided.")
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0 when provided.")


WorkflowReference = str | type[Any]
