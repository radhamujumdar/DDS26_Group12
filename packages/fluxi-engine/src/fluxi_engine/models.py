"""Shared internal models for the Fluxi engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


TerminalStatus = Literal["completed", "failed"]
StartDecision = Literal["started", "attached", "existing_terminal", "duplicate_rejected"]
WorkflowCommandKind = Literal[
    "schedule_activity",
    "record_local_activity",
    "complete_workflow",
    "fail_workflow",
]
ActivityCompletionStatus = Literal["completed", "failed"]


@dataclass(frozen=True, slots=True)
class RetryPolicyConfig:
    max_attempts: int | None = None
    initial_interval_ms: int | None = None
    backoff_coefficient: float | None = None
    max_interval_ms: int | None = None


@dataclass(frozen=True, slots=True)
class StartWorkflowResult:
    decision: StartDecision
    run_id: str | None
    status: str
    run_no: int | None
    result_payload: bytes | None = None
    error_payload: bytes | None = None


@dataclass(frozen=True, slots=True)
class WorkflowResultSnapshot:
    workflow_id: str
    run_id: str | None
    status: str
    run_no: int | None
    result_payload: bytes | None = None
    error_payload: bytes | None = None


@dataclass(frozen=True, slots=True)
class WorkflowTaskCommand:
    kind: WorkflowCommandKind
    activity_execution_id: str | None = None
    activity_name: str | None = None
    activity_task_queue: str | None = None
    activity_status: ActivityCompletionStatus | None = None
    input_payload: bytes | None = None
    retry_policy: RetryPolicyConfig | None = None
    schedule_to_close_timeout_ms: int | None = None
    result_payload: bytes | None = None
    error_payload: bytes | None = None


@dataclass(frozen=True, slots=True)
class WorkflowTaskCompletionResult:
    outcome: str
    activity_execution_id: str | None = None
    activity_execution_ids: tuple[str, ...] = ()
    run_id: str | None = None
    terminal_status: str | None = None


@dataclass(frozen=True, slots=True)
class ActivityTaskCompletionResult:
    outcome: str
    run_id: str | None = None
    activity_execution_id: str | None = None
    next_workflow_task_id: str | None = None
