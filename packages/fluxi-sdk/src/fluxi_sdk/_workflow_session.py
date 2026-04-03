from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
import inspect
import logging
import time
from typing import Any

from fluxi_engine.observability import elapsed_ms, trace_logging_enabled

from . import activity, workflow
from ._codec import (
    decode_args_payload,
    decode_failure_payload,
    decode_payload,
    encode_args_payload,
    encode_failure_payload,
    encode_payload,
)
from .activity import ActivityRegistration
from .errors import NonDeterministicWorkflowError, RemoteActivityError
from .types import ActivityOptions, RetryPolicy
from .workflow import WorkflowRegistration

logger = logging.getLogger(__name__)
TRACE_LOGGING_ENABLED = trace_logging_enabled()


@dataclass(frozen=True, slots=True)
class WorkflowDriveResult:
    status: str
    commands: list[dict[str, Any]]
    waiting_remote_execution_id: str | None = None
    result_payload: bytes | None = None
    failure_payload: bytes | None = None


@dataclass(frozen=True, slots=True)
class _RetryPolicySnapshot:
    max_attempts: int
    initial_interval_ms: int
    backoff_coefficient: float
    max_interval_ms: int


@dataclass(frozen=True, slots=True)
class _RecordedRemoteActivity:
    activity_execution_id: str
    activity_name: str
    task_queue: str
    input_payload: bytes
    schedule_to_close_timeout_ms: int | None
    retry_policy: _RetryPolicySnapshot
    terminal_event: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class _RecordedLocalActivity:
    activity_execution_id: str
    activity_name: str
    input_payload: bytes
    schedule_to_close_timeout_ms: int | None
    retry_policy: _RetryPolicySnapshot
    status: str
    result_payload: bytes | None
    error_payload: bytes | None


RecordedActivityStep = _RecordedRemoteActivity | _RecordedLocalActivity


@dataclass(slots=True)
class _PendingLocalActivity:
    activity_execution_id: str
    activity_name: str
    args: tuple[Any, ...]
    options: ActivityOptions
    input_payload: bytes
    command_index: int
    task: asyncio.Task[Any] | None = None


def build_session_from_history(
    *,
    run_id: str,
    workflow_id: str,
    task_queue: str,
    history: list[dict[str, Any]],
    history_index: int,
    registration: WorkflowRegistration,
    activity_by_name: dict[str, ActivityRegistration],
) -> "WorkflowSession":
    if not history or history[0].get("event_type") != "WorkflowStarted":
        raise NonDeterministicWorkflowError(
            "Workflow history must begin with WorkflowStarted."
        )
    run_method = getattr(registration.workflow(), registration.run_method_name)
    args = decode_args_payload(history[0].get("input_payload"), run_method)
    recorded_steps, next_activity_sequence_no = _recorded_activity_steps(history)
    return WorkflowSession(
        run_id=run_id,
        workflow_id=workflow_id,
        task_queue=task_queue,
        registration=registration,
        args=args,
        activity_by_name=activity_by_name,
        recorded_steps=recorded_steps,
        next_activity_sequence_no=next_activity_sequence_no,
        last_history_index=history_index,
    )


@dataclass(slots=True)
class WorkflowSession:
    run_id: str
    workflow_id: str
    task_queue: str
    registration: WorkflowRegistration
    args: tuple[Any, ...]
    activity_by_name: dict[str, ActivityRegistration]
    recorded_steps: tuple[RecordedActivityStep, ...]
    next_activity_sequence_no: int
    last_history_index: int
    _workflow_instance: Any = field(init=False, repr=False)
    _run_method: Any = field(init=False, repr=False)
    _workflow_task: asyncio.Task[Any] = field(init=False, repr=False)
    _call_index: int = field(init=False, default=0, repr=False)
    _pending_commands: list[dict[str, Any] | None] = field(
        init=False,
        default_factory=list,
        repr=False,
    )
    _remote_futures: dict[str, asyncio.Future[Any]] = field(
        init=False,
        default_factory=dict,
        repr=False,
    )
    _waiting_on_remote: asyncio.Future[Any] | None = field(
        init=False,
        default=None,
        repr=False,
    )
    _waiting_remote_execution_id: str | None = field(
        init=False,
        default=None,
        repr=False,
    )
    _local_tasks: set[asyncio.Task[Any]] = field(
        init=False,
        default_factory=set,
        repr=False,
    )

    def __post_init__(self) -> None:
        self._workflow_instance = self.registration.workflow()
        self._run_method = getattr(self._workflow_instance, self.registration.run_method_name)
        self._workflow_task = asyncio.create_task(
            self._run_workflow(),
            name=f"fluxi-sdk:workflow-session:{self.workflow_id}:{self.run_id}",
        )

    async def cancel(self) -> None:
        self._workflow_task.cancel()
        if self._local_tasks:
            for task in list(self._local_tasks):
                task.cancel()
            await asyncio.gather(*self._local_tasks, return_exceptions=True)
        await asyncio.gather(self._workflow_task, return_exceptions=True)

    def apply_history_events(
        self,
        events: list[dict[str, Any]],
        *,
        next_index: int,
    ) -> None:
        for event in events:
            event_type = str(event.get("event_type"))
            if event_type not in {"ActivityCompleted", "ActivityFailed"}:
                continue
            execution_id = str(event["activity_execution_id"])
            future = self._remote_futures.get(execution_id)
            if future is None or future.done():
                continue
            if event_type == "ActivityCompleted":
                future.set_result(decode_payload(event.get("result_payload")))
            else:
                future.set_exception(
                    decode_failure_payload(
                        event.get("error_payload"),
                        fallback_error=RemoteActivityError,
                    )
                )
        self.last_history_index = next_index

    async def drive(self) -> WorkflowDriveResult:
        self._pending_commands = []
        await self._advance_until_idle()

        commands = [command for command in self._pending_commands if command is not None]
        if self._workflow_task.done():
            error = self._workflow_task.exception()
            if error is not None:
                return WorkflowDriveResult(
                    status="failed",
                    commands=commands,
                    failure_payload=_safe_failure_payload(error),
                )
            return WorkflowDriveResult(
                status="completed",
                commands=commands,
                result_payload=encode_payload(self._workflow_task.result()),
            )

        return WorkflowDriveResult(
            status="suspended",
            commands=commands,
            waiting_remote_execution_id=self._waiting_remote_execution_id,
        )

    async def _run_workflow(self) -> Any:
        with workflow._activate_execution_context(
            self.registration,
            self.task_queue,
            self.start_remote_activity,
            self.start_local_activity,
        ):
            return await self._run_method(*self.args)

    async def _advance_until_idle(self) -> None:
        iterations = 0
        while True:
            iterations += 1
            if iterations > 10_000:
                raise RuntimeError(
                    f"Workflow session {self.workflow_id!r} failed to reach an idle state."
                )

            await asyncio.sleep(0)

            if self._local_tasks:
                done, pending = await asyncio.wait(
                    list(self._local_tasks),
                    timeout=0,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done:
                    await asyncio.gather(*done, return_exceptions=True)
                    if pending:
                        self._local_tasks = set(pending)
                    else:
                        self._local_tasks.clear()
                    continue

            if self._workflow_task.done():
                if self._local_tasks:
                    continue
                return

            if self._waiting_on_remote is not None and not self._waiting_on_remote.done():
                if self._local_tasks:
                    continue
                return

    def start_remote_activity(
        self,
        activity_name: str,
        args: tuple[Any, ...],
        options: ActivityOptions,
    ) -> workflow.ActivityHandle[Any]:
        input_payload = encode_args_payload(args)
        retry_policy = _retry_policy_snapshot(options.retry_policy)
        step = self._next_recorded_step()
        if step is not None:
            if not isinstance(step, _RecordedRemoteActivity):
                raise NonDeterministicWorkflowError(
                    f"Expected local activity {step.activity_name!r}, got remote activity {activity_name!r}."
                )
            _assert_activity_metadata(
                step.activity_name,
                activity_name,
                step.input_payload,
                input_payload,
                step.schedule_to_close_timeout_ms,
                _timedelta_to_ms(options.schedule_to_close_timeout),
                step.retry_policy,
                retry_policy,
                actual_task_queue=step.task_queue,
                expected_task_queue=options.task_queue or self.task_queue,
            )
            future = self._remote_futures.get(step.activity_execution_id)
            if future is None:
                future = asyncio.get_running_loop().create_future()
                self._remote_futures[step.activity_execution_id] = future
            if step.terminal_event is not None and not future.done():
                self._apply_remote_terminal_event(
                    step.activity_execution_id,
                    step.terminal_event,
                )
            return workflow.ActivityHandle(
                lambda: self._await_remote_activity(
                    step.activity_execution_id,
                    future,
                )
            )

        activity_execution_id = self._allocate_activity_execution_id()
        future = asyncio.get_running_loop().create_future()
        self._remote_futures[activity_execution_id] = future
        self._pending_commands.append(
            {
                "kind": "schedule_activity",
                "activity_execution_id": activity_execution_id,
                "activity_name": activity_name,
                "activity_task_queue": options.task_queue or self.task_queue,
                "input_payload": input_payload,
                "retry_policy": options.retry_policy,
                "schedule_to_close_timeout_ms": _timedelta_to_ms(
                    options.schedule_to_close_timeout
                ),
            }
        )
        return workflow.ActivityHandle(
            lambda: self._await_remote_activity(
                activity_execution_id,
                future,
            )
        )

    def start_local_activity(
        self,
        activity_name: str,
        args: tuple[Any, ...],
        options: ActivityOptions,
    ) -> workflow.ActivityHandle[Any]:
        input_payload = encode_args_payload(args)
        retry_policy = _retry_policy_snapshot(options.retry_policy)
        step = self._next_recorded_step()
        if step is not None:
            if not isinstance(step, _RecordedLocalActivity):
                raise NonDeterministicWorkflowError(
                    f"Expected remote activity {step.activity_name!r}, got local activity {activity_name!r}."
                )
            _assert_activity_metadata(
                step.activity_name,
                activity_name,
                step.input_payload,
                input_payload,
                step.schedule_to_close_timeout_ms,
                _timedelta_to_ms(options.schedule_to_close_timeout),
                step.retry_policy,
                retry_policy,
            )
            future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
            if step.status == "completed":
                future.set_result(decode_payload(step.result_payload))
            else:
                future.set_exception(
                    decode_failure_payload(
                        step.error_payload,
                        fallback_error=RemoteActivityError,
                    )
                )
            return workflow.ActivityHandle(lambda: future)

        activity_execution_id = self._allocate_activity_execution_id()
        command_index = len(self._pending_commands)
        self._pending_commands.append(None)
        local_state = _PendingLocalActivity(
            activity_execution_id=activity_execution_id,
            activity_name=activity_name,
            args=args,
            options=options,
            input_payload=input_payload,
            command_index=command_index,
        )
        local_state.task = asyncio.create_task(
            self._run_new_local_activity(local_state),
            name=(
                "fluxi-sdk:local-activity:"
                f"{self.workflow_id}:{activity_execution_id}:{activity_name}"
            ),
        )
        self._local_tasks.add(local_state.task)

        def _done(task: asyncio.Task[Any]) -> None:
            self._local_tasks.discard(task)
            if not task.cancelled():
                task.exception()

        local_state.task.add_done_callback(_done)
        return workflow.ActivityHandle(lambda: local_state.task)

    async def _await_remote_activity(
        self,
        activity_execution_id: str,
        future: asyncio.Future[Any],
    ) -> Any:
        if future.done():
            return await future
        self._waiting_on_remote = future
        self._waiting_remote_execution_id = activity_execution_id
        try:
            return await future
        finally:
            if self._waiting_on_remote is future:
                self._waiting_on_remote = None
                self._waiting_remote_execution_id = None

    async def _run_new_local_activity(self, state: _PendingLocalActivity) -> Any:
        activity_start = time.perf_counter()
        registration = self.activity_by_name.get(state.activity_name)
        if registration is None:
            error = RemoteActivityError(
                f"Worker for task queue {self.task_queue!r} cannot execute local activity {state.activity_name!r}."
            )
            self._pending_commands[state.command_index] = _local_activity_command(
                state=state,
                retry_policy=state.options.retry_policy,
                status="failed",
                result_payload=None,
                error_payload=encode_failure_payload(error),
            )
            raise error

        retry_policy = _retry_policy_snapshot(state.options.retry_policy)
        attempt_no = 1
        deadline = None
        if state.options.schedule_to_close_timeout is not None:
            deadline = (
                time.monotonic() + state.options.schedule_to_close_timeout.total_seconds()
            )

        while True:
            try:
                result = await _execute_local_activity_attempt(
                    registration=registration,
                    activity_execution_id=state.activity_execution_id,
                    activity_name=state.activity_name,
                    task_queue=self.task_queue,
                    workflow_id=self.workflow_id,
                    run_id=self.run_id,
                    args=state.args,
                    attempt_no=attempt_no,
                    deadline=deadline,
                )
            except BaseException as exc:
                should_retry = attempt_no < retry_policy.max_attempts
                if should_retry and deadline is not None and time.monotonic() >= deadline:
                    should_retry = False
                if should_retry:
                    delay_ms = _retry_delay_ms(retry_policy, attempt_no)
                    if TRACE_LOGGING_ENABLED:
                        logger.info(
                            "workflow.local_activity.retry workflow_id=%s run_id=%s activity_execution_id=%s activity_name=%s attempt_no=%d delay_ms=%d",
                            self.workflow_id,
                            self.run_id,
                            state.activity_execution_id,
                            state.activity_name,
                            attempt_no,
                            delay_ms,
                        )
                    if delay_ms > 0:
                        await asyncio.sleep(delay_ms / 1000)
                    attempt_no += 1
                    continue

                self._pending_commands[state.command_index] = _local_activity_command(
                    state=state,
                    retry_policy=state.options.retry_policy,
                    status="failed",
                    result_payload=None,
                    error_payload=encode_failure_payload(exc),
                )
                if TRACE_LOGGING_ENABLED:
                    logger.info(
                        "workflow.local_activity.failed workflow_id=%s run_id=%s activity_execution_id=%s activity_name=%s attempts=%d duration_ms=%.2f error_type=%s",
                        self.workflow_id,
                        self.run_id,
                        state.activity_execution_id,
                        state.activity_name,
                        attempt_no,
                        elapsed_ms(activity_start),
                        exc.__class__.__name__,
                    )
                raise

            self._pending_commands[state.command_index] = _local_activity_command(
                state=state,
                retry_policy=state.options.retry_policy,
                status="completed",
                result_payload=encode_payload(result),
                error_payload=None,
            )
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "workflow.local_activity.completed workflow_id=%s run_id=%s activity_execution_id=%s activity_name=%s attempts=%d duration_ms=%.2f",
                    self.workflow_id,
                    self.run_id,
                    state.activity_execution_id,
                    state.activity_name,
                    attempt_no,
                    elapsed_ms(activity_start),
                )
            return result

    def _apply_remote_terminal_event(
        self,
        activity_execution_id: str,
        event: dict[str, Any],
    ) -> None:
        future = self._remote_futures.get(activity_execution_id)
        if future is None or future.done():
            return
        event_type = str(event.get("event_type"))
        if event_type == "ActivityCompleted":
            future.set_result(decode_payload(event.get("result_payload")))
            return
        if event_type == "ActivityFailed":
            future.set_exception(
                decode_failure_payload(
                    event.get("error_payload"),
                    fallback_error=RemoteActivityError,
                )
            )
            return
        raise NonDeterministicWorkflowError(
            f"Unsupported remote activity terminal event {event_type!r}."
        )

    def _allocate_activity_execution_id(self) -> str:
        execution_id = f"{self.run_id}:act:{self.next_activity_sequence_no}"
        self.next_activity_sequence_no += 1
        return execution_id

    def _next_recorded_step(self) -> RecordedActivityStep | None:
        step_index = self._call_index
        self._call_index += 1
        if step_index >= len(self.recorded_steps):
            return None
        return self.recorded_steps[step_index]


async def _execute_local_activity_attempt(
    *,
    registration: ActivityRegistration,
    activity_execution_id: str,
    activity_name: str,
    task_queue: str,
    workflow_id: str,
    run_id: str,
    args: tuple[Any, ...],
    attempt_no: int,
    deadline: float | None,
) -> Any:
    with activity._activate_execution_context(
        activity.ActivityExecutionInfo(
            activity_execution_id=activity_execution_id,
            attempt_no=attempt_no,
            activity_name=activity_name,
            task_queue=task_queue,
            workflow_id=workflow_id,
            run_id=run_id,
        )
    ):
        result = registration.fn(*args)
        if inspect.isawaitable(result):
            remaining = None
            if deadline is not None:
                remaining = max(deadline - time.monotonic(), 0)
            if remaining is not None:
                result = await asyncio.wait_for(result, timeout=remaining)
            else:
                result = await result
        elif deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError(
                f"Local activity {activity_name!r} exceeded its schedule_to_close_timeout."
            )
    return result


def _recorded_activity_steps(
    history: list[dict[str, Any]],
) -> tuple[tuple[RecordedActivityStep, ...], int]:
    terminal_remote_events: dict[str, dict[str, Any]] = {}
    for event in history:
        event_type = str(event.get("event_type"))
        if event_type in {"ActivityCompleted", "ActivityFailed"}:
            terminal_remote_events[str(event["activity_execution_id"])] = event

    steps: list[RecordedActivityStep] = []
    next_sequence_no = 1
    for event in history:
        event_type = str(event.get("event_type"))
        if event_type == "ActivityScheduled":
            activity_execution_id = str(event["activity_execution_id"])
            next_sequence_no = max(
                next_sequence_no,
                _activity_sequence_no(activity_execution_id) + 1,
            )
            steps.append(
                _RecordedRemoteActivity(
                    activity_execution_id=activity_execution_id,
                    activity_name=str(event["activity_name"]),
                    task_queue=str(event["task_queue"]),
                    input_payload=event.get("input_payload") or b"",
                    schedule_to_close_timeout_ms=_int_or_none(
                        event.get("schedule_to_close_timeout_ms")
                    ),
                    retry_policy=_retry_policy_snapshot_from_history(event),
                    terminal_event=terminal_remote_events.get(activity_execution_id),
                )
            )
        elif event_type == "LocalActivityRecorded":
            activity_execution_id = str(event["activity_execution_id"])
            next_sequence_no = max(
                next_sequence_no,
                _activity_sequence_no(activity_execution_id) + 1,
            )
            steps.append(
                _RecordedLocalActivity(
                    activity_execution_id=activity_execution_id,
                    activity_name=str(event["activity_name"]),
                    input_payload=event.get("input_payload") or b"",
                    schedule_to_close_timeout_ms=_int_or_none(
                        event.get("schedule_to_close_timeout_ms")
                    ),
                    retry_policy=_retry_policy_snapshot_from_history(event),
                    status=str(event["status"]),
                    result_payload=event.get("result_payload"),
                    error_payload=event.get("error_payload"),
                )
            )
    return tuple(steps), next_sequence_no


def _retry_policy_snapshot_from_history(event: dict[str, Any]) -> _RetryPolicySnapshot:
    return _RetryPolicySnapshot(
        max_attempts=int(event.get("max_attempts") or 1),
        initial_interval_ms=int(event.get("initial_interval_ms") or 0),
        backoff_coefficient=float(event.get("backoff_coefficient") or 0.0),
        max_interval_ms=int(event.get("max_interval_ms") or 0),
    )


def _retry_policy_snapshot(retry_policy: RetryPolicy | None) -> _RetryPolicySnapshot:
    if retry_policy is None:
        return _RetryPolicySnapshot(
            max_attempts=1,
            initial_interval_ms=0,
            backoff_coefficient=0.0,
            max_interval_ms=0,
        )
    return _RetryPolicySnapshot(
        max_attempts=retry_policy.max_attempts or 1,
        initial_interval_ms=(
            int(retry_policy.initial_interval_seconds * 1000)
            if retry_policy.initial_interval_seconds is not None
            else 0
        ),
        backoff_coefficient=float(retry_policy.backoff_coefficient or 0.0),
        max_interval_ms=(
            int(retry_policy.max_interval_seconds * 1000)
            if retry_policy.max_interval_seconds is not None
            else 0
        ),
    )


def _retry_delay_ms(retry_policy: _RetryPolicySnapshot, attempt_no: int) -> int:
    delay_ms = retry_policy.initial_interval_ms
    if delay_ms <= 0:
        delay_ms = 1
    if attempt_no > 1 and retry_policy.backoff_coefficient > 1:
        delay_ms = int(
            delay_ms * (retry_policy.backoff_coefficient ** (attempt_no - 1))
        )
    if retry_policy.max_interval_ms > 0:
        delay_ms = min(delay_ms, retry_policy.max_interval_ms)
    return delay_ms


def _local_activity_command(
    *,
    state: _PendingLocalActivity,
    retry_policy: RetryPolicy | None,
    status: str,
    result_payload: bytes | None,
    error_payload: bytes | None,
) -> dict[str, Any]:
    return {
        "kind": "record_local_activity",
        "activity_execution_id": state.activity_execution_id,
        "activity_name": state.activity_name,
        "activity_status": status,
        "input_payload": state.input_payload,
        "retry_policy": retry_policy,
        "schedule_to_close_timeout_ms": _timedelta_to_ms(
            state.options.schedule_to_close_timeout
        ),
        "result_payload": result_payload,
        "error_payload": error_payload,
    }


def _assert_activity_metadata(
    recorded_activity_name: str,
    expected_activity_name: str,
    recorded_input_payload: bytes | None,
    expected_input_payload: bytes | None,
    recorded_timeout_ms: int | None,
    expected_timeout_ms: int | None,
    recorded_retry_policy: _RetryPolicySnapshot,
    expected_retry_policy: _RetryPolicySnapshot,
    *,
    actual_task_queue: str | None = None,
    expected_task_queue: str | None = None,
) -> None:
    if recorded_activity_name != expected_activity_name:
        raise NonDeterministicWorkflowError(
            f"Expected activity {recorded_activity_name!r}, got {expected_activity_name!r}."
        )
    if actual_task_queue is not None and actual_task_queue != expected_task_queue:
        raise NonDeterministicWorkflowError(
            f"Activity {expected_activity_name!r} used task queue {expected_task_queue!r}, "
            f"but history recorded {actual_task_queue!r}."
        )
    if recorded_input_payload != expected_input_payload:
        raise NonDeterministicWorkflowError(
            f"Activity {expected_activity_name!r} input payload does not match stored history."
        )
    if int(recorded_timeout_ms or 0) != int(expected_timeout_ms or 0):
        raise NonDeterministicWorkflowError(
            f"Activity {expected_activity_name!r} timeout does not match stored history."
        )
    if recorded_retry_policy != expected_retry_policy:
        raise NonDeterministicWorkflowError(
            f"Activity {expected_activity_name!r} retry policy does not match stored history."
        )


def _activity_sequence_no(activity_execution_id: str) -> int:
    try:
        return int(activity_execution_id.rsplit(":act:", 1)[1])
    except Exception as exc:
        raise NonDeterministicWorkflowError(
            f"Activity execution id {activity_execution_id!r} does not follow the expected pattern."
        ) from exc


def _timedelta_to_ms(value: timedelta | None) -> int | None:
    if value is None:
        return None
    return int(value.total_seconds() * 1000)


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _safe_failure_payload(exc: BaseException) -> bytes:
    try:
        return encode_failure_payload(exc)
    except Exception as codec_error:
        fallback = RemoteActivityError(
            f"Failed to encode exception {exc.__class__.__name__}: {codec_error}"
        )
        return encode_failure_payload(fallback)
