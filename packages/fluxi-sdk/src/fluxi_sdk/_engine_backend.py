"""Engine-backed SDK runtime using fluxi-server plus Redis Streams."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
import inspect
import logging
import os
import socket
from typing import Any, Callable
import uuid

import httpx
from redis.asyncio.sentinel import MasterNotFoundError
from redis.exceptions import ConnectionError, ReadOnlyError, ResponseError, TimeoutError

from fluxi_engine.codecs import from_base64, to_base64, unpackb
from fluxi_engine.config import FluxiSettings, create_redis_client
from fluxi_engine.keys import activity_queue, workflow_queue
from fluxi_engine.store import FluxiRedisStore

from . import activity, workflow
from ._codec import (
    decode_args_payload,
    decode_failure_payload,
    decode_payload,
    decode_return_payload,
    encode_args_payload,
    encode_failure_payload,
    encode_payload,
)
from .activity import ActivityRegistration
from .client import EngineConnectionConfig
from .errors import (
    NonDeterministicWorkflowError,
    RemoteActivityError,
    RemoteWorkflowError,
    WorkflowAlreadyStartedError,
)
from .types import ActivityOptions, RetryPolicy, StartPolicy, WorkflowReference
from .workflow import WorkflowRegistration, _get_workflow_name, _get_workflow_run_callable

_WORKFLOW_ACK_OUTCOMES = {"scheduled_activity", "completed", "stale", "conflict", "missing"}
_ACTIVITY_ACK_OUTCOMES = {"accepted", "stale", "conflict", "missing"}
_ATTEMPT_EVENT_TYPES = {
    "ActivityAttemptFailed",
    "ActivityAttemptTimedOut",
    "ActivityRetryScheduled",
}
_PENDING_RESULT = object()
_RESULT_RETRYABLE_HTTP_STATUS_CODES = {404, 500, 502, 503, 504}
_REDIS_RECOVERABLE_ERRORS = (
    ConnectionError,
    TimeoutError,
    MasterNotFoundError,
    ReadOnlyError,
)

logger = logging.getLogger(__name__)


class EngineWorkflowBackend:
    def __init__(self, config: EngineConnectionConfig) -> None:
        self._config = config

    async def _execute_workflow(
        self,
        workflow_ref: WorkflowReference,
        *,
        workflow_id: str,
        task_queue: str,
        args: tuple[Any, ...],
        start_policy: StartPolicy,
    ) -> Any:
        workflow_name = _get_workflow_name(workflow_ref)
        run_callable = _get_workflow_run_callable(workflow_ref)
        input_payload = encode_args_payload(args)

        async with httpx.AsyncClient(base_url=self._config.server_url.rstrip("/")) as client:
            start_request = {
                "workflow_id": workflow_id,
                "workflow_name": workflow_name,
                "task_queue": task_queue,
                "start_policy": start_policy.value,
                "input_payload_b64": to_base64(input_payload),
            }
            body = await self._start_or_attach(
                client,
                request=start_request,
                start_policy=start_policy,
            )
            terminal = _resolve_terminal_start_response(
                body,
                workflow_id=workflow_id,
            )
            if terminal is not _PENDING_RESULT:
                if terminal["status"] == "completed":
                    return _decode_workflow_result(
                        terminal.get("result_payload_b64"),
                        run_callable,
                    )
                if terminal["status"] == "failed":
                    raise _decode_workflow_failure(terminal.get("error_payload_b64"))

            while True:
                snapshot = await self._poll_workflow_result(
                    client,
                    workflow_id=workflow_id,
                    start_policy=start_policy,
                    start_request=start_request,
                )
                if snapshot is _PENDING_RESULT:
                    continue
                if snapshot["status"] == "completed":
                    return _decode_workflow_result(
                        snapshot.get("result_payload_b64"),
                        run_callable,
                    )
                if snapshot["status"] == "failed":
                    raise _decode_workflow_failure(snapshot.get("error_payload_b64"))

    async def _start_or_attach(
        self,
        client: httpx.AsyncClient,
        *,
        request: dict[str, Any],
        start_policy: StartPolicy,
    ) -> dict[str, Any]:
        while True:
            try:
                response = await client.post("/workflows/start-or-attach", json=request)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                if not _should_retry_start_request(start_policy, exc.response.status_code):
                    raise
            except httpx.HTTPError:
                if not _should_retry_start_request(start_policy, None):
                    raise
            await asyncio.sleep(self._config.result_poll_interval_ms / 1000)

    async def _poll_workflow_result(
        self,
        client: httpx.AsyncClient,
        *,
        workflow_id: str,
        start_policy: StartPolicy,
        start_request: dict[str, Any],
    ) -> dict[str, Any] | object:
        while True:
            try:
                result_response = await client.get(
                    f"/workflows/{workflow_id}/result",
                    params={"wait_ms": self._config.result_poll_interval_ms},
                )
                if result_response.status_code == 404 and _should_retry_start_request(
                    start_policy,
                    404,
                ):
                    body = await self._start_or_attach(
                        client,
                        request=start_request,
                        start_policy=start_policy,
                    )
                    terminal = _resolve_terminal_start_response(
                        body,
                        workflow_id=workflow_id,
                    )
                    if terminal is not _PENDING_RESULT:
                        return terminal
                    await asyncio.sleep(self._config.result_poll_interval_ms / 1000)
                    return _PENDING_RESULT
                result_response.raise_for_status()
                return result_response.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in _RESULT_RETRYABLE_HTTP_STATUS_CODES:
                    await asyncio.sleep(self._config.result_poll_interval_ms / 1000)
                    continue
                raise
            except httpx.HTTPError:
                await asyncio.sleep(self._config.result_poll_interval_ms / 1000)

    def _create_worker_binding(
        self,
        *,
        task_queue: str,
        workflows: Sequence[type[Any]],
        activities: Sequence[Callable[..., Any]],
    ) -> _EngineWorkerBinding:
        workflow_registrations = _register_workflows(workflows)
        activity_registrations = _register_activities(activities)
        return _EngineWorkerBinding(
            config=self._config,
            task_queue=task_queue,
            workflows=workflow_registrations,
            activities=activity_registrations,
        )


@dataclass(slots=True)
class _EngineWorkerBinding:
    config: EngineConnectionConfig
    task_queue: str
    workflows: tuple[WorkflowRegistration, ...]
    activities: tuple[ActivityRegistration, ...]
    _settings: FluxiSettings = field(init=False, repr=False)
    _workflow_by_name: dict[str, WorkflowRegistration] = field(init=False, repr=False)
    _activity_by_name: dict[str, ActivityRegistration] = field(init=False, repr=False)
    _consumer_name: str = field(init=False, repr=False)
    _stop_event: asyncio.Event = field(init=False, repr=False)
    _stopped_event: asyncio.Event = field(init=False, repr=False)
    _redis_store: FluxiRedisStore | None = field(init=False, default=None, repr=False)
    _http_client: httpx.AsyncClient | None = field(init=False, default=None, repr=False)
    _tasks: list[asyncio.Task[None]] = field(init=False, default_factory=list, repr=False)
    _running: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        self._settings = FluxiSettings(
            redis_mode=self.config.redis_mode,
            redis_url=self.config.redis_url,
            sentinel_endpoints=self.config.sentinel_endpoints,
            sentinel_service_name=self.config.sentinel_service_name,
            sentinel_min_other_sentinels=self.config.sentinel_min_other_sentinels,
            sentinel_username=self.config.sentinel_username,
            sentinel_password=self.config.sentinel_password,
            key_prefix=self.config.key_prefix,
            workflow_consumer_group=self.config.workflow_consumer_group,
            activity_consumer_group=self.config.activity_consumer_group,
            result_poll_interval_ms=self.config.result_poll_interval_ms,
        )
        self._workflow_by_name = {registration.name: registration for registration in self.workflows}
        self._activity_by_name = {registration.name: registration for registration in self.activities}
        self._consumer_name = (
            f"fluxi-sdk:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        )
        self._stop_event = asyncio.Event()
        self._stopped_event = asyncio.Event()
        self._stopped_event.set()
        self._redis_store: FluxiRedisStore | None = None
        self._http_client: httpx.AsyncClient | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False

    async def start(self) -> None:
        if self._running:
            return

        self._stop_event = asyncio.Event()
        self._stopped_event = asyncio.Event()
        redis = create_redis_client(self._settings)
        self._redis_store = FluxiRedisStore(redis, self._settings)
        self._http_client = httpx.AsyncClient(base_url=self.config.server_url.rstrip("/"))

        if self.workflows:
            await self._redis_store.ensure_workflow_queue(self.task_queue)
            self._tasks.append(
                asyncio.create_task(
                    self._workflow_loop(),
                    name=f"fluxi-sdk:workflow:{self.task_queue}:{self._consumer_name}",
                )
            )
        if self.activities:
            await self._redis_store.ensure_activity_queue(self.task_queue)
            self._tasks.append(
                asyncio.create_task(
                    self._activity_loop(),
                    name=f"fluxi-sdk:activity:{self.task_queue}:{self._consumer_name}",
                )
            )

        self._running = True

    async def wait(self) -> None:
        await self._stopped_event.wait()

    async def shutdown(self) -> None:
        if not self._running:
            return
        self._stop_event.set()
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._http_client is not None:
            await self._http_client.aclose()
        if self._redis_store is not None:
            await self._redis_store.aclose()
        self._http_client = None
        self._redis_store = None
        self._running = False
        self._stopped_event.set()

    async def _workflow_loop(self) -> None:
        assert self._redis_store is not None
        stream_key = workflow_queue(self._settings.key_prefix, self.task_queue)
        try:
            while not self._stop_event.is_set():
                try:
                    payload = await _read_stream_payload(
                        self._redis_store,
                        stream_key=stream_key,
                        group_name=self._settings.workflow_consumer_group,
                        consumer_name=self._consumer_name,
                    )
                    if payload is None:
                        continue
                    message_id, task_payload = payload
                    should_ack = await self._process_workflow_task(task_payload)
                    if should_ack:
                        await self._redis_store.redis.xack(
                            stream_key,
                            self._settings.workflow_consumer_group,
                            message_id,
                        )
                except _REDIS_RECOVERABLE_ERRORS as exc:
                    logger.warning(
                        "Transient Redis/Sentinel error in Fluxi workflow worker loop for %s: %s",
                        self.task_queue,
                        exc,
                    )
                    await self._wait_for_retry()
                except ResponseError as exc:
                    if _is_missing_stream_group_error(exc):
                        logger.warning(
                            "Recreating missing workflow consumer group for %s after Redis failover.",
                            self.task_queue,
                        )
                        await self._ensure_workflow_group()
                        await self._wait_for_retry(0.05)
                        continue
                    raise
                except httpx.HTTPError as exc:
                    logger.warning(
                        "Transient HTTP error in Fluxi workflow worker loop for %s: %s",
                        self.task_queue,
                        exc,
                    )
                    await self._wait_for_retry()
                except Exception:
                    logger.exception(
                        "Unexpected error in Fluxi workflow worker loop for %s.",
                        self.task_queue,
                    )
                    await self._wait_for_retry()
        except asyncio.CancelledError:
            raise

    async def _activity_loop(self) -> None:
        assert self._redis_store is not None
        stream_key = activity_queue(self._settings.key_prefix, self.task_queue)
        try:
            while not self._stop_event.is_set():
                try:
                    payload = await _read_stream_payload(
                        self._redis_store,
                        stream_key=stream_key,
                        group_name=self._settings.activity_consumer_group,
                        consumer_name=self._consumer_name,
                    )
                    if payload is None:
                        continue
                    message_id, task_payload = payload
                    should_ack = await self._process_activity_task(task_payload)
                    if should_ack:
                        await self._redis_store.redis.xack(
                            stream_key,
                            self._settings.activity_consumer_group,
                            message_id,
                        )
                except _REDIS_RECOVERABLE_ERRORS as exc:
                    logger.warning(
                        "Transient Redis/Sentinel error in Fluxi activity worker loop for %s: %s",
                        self.task_queue,
                        exc,
                    )
                    await self._wait_for_retry()
                except ResponseError as exc:
                    if _is_missing_stream_group_error(exc):
                        logger.warning(
                            "Recreating missing activity consumer group for %s after Redis failover.",
                            self.task_queue,
                        )
                        await self._ensure_activity_group()
                        await self._wait_for_retry(0.05)
                        continue
                    raise
                except httpx.HTTPError as exc:
                    logger.warning(
                        "Transient HTTP error in Fluxi activity worker loop for %s: %s",
                        self.task_queue,
                        exc,
                    )
                    await self._wait_for_retry()
                except Exception:
                    logger.exception(
                        "Unexpected error in Fluxi activity worker loop for %s.",
                        self.task_queue,
                    )
                    await self._wait_for_retry()
        except asyncio.CancelledError:
            raise

    async def _wait_for_retry(self, delay_seconds: float = 0.5) -> None:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay_seconds)
        except asyncio.TimeoutError:
            return

    async def _ensure_workflow_group(self) -> None:
        assert self._redis_store is not None
        await self._redis_store.ensure_workflow_queue(self.task_queue)

    async def _ensure_activity_group(self) -> None:
        assert self._redis_store is not None
        await self._redis_store.ensure_activity_queue(self.task_queue)

    async def _process_workflow_task(self, task_payload: dict[str, Any]) -> bool:
        registration = self._workflow_by_name.get(str(task_payload["workflow_name"]))
        if registration is None:
            failure_payload = _safe_failure_payload(
                RemoteWorkflowError(
                    f"Worker for task queue {self.task_queue!r} cannot execute workflow {task_payload['workflow_name']!r}."
                )
            )
            outcome = await self._submit_workflow_failure(
                run_id=str(task_payload["run_id"]),
                workflow_task_id=str(task_payload["workflow_task_id"]),
                attempt_no=int(task_payload["attempt_no"]),
                error_payload=failure_payload,
            )
            return outcome in _WORKFLOW_ACK_OUTCOMES

        history = await self._fetch_run_history(str(task_payload["run_id"]))
        controller = _WorkflowReplayController(history, task_queue=self.task_queue)
        workflow_instance = registration.workflow()
        run_method = getattr(workflow_instance, registration.run_method_name)
        args = decode_args_payload(controller.workflow_input_payload, run_method)

        try:
            with workflow._activate_execution_context(
                registration,
                self.task_queue,
                controller.execute_activity,
            ):
                result = await run_method(*args)
            controller.assert_fully_replayed()
            result_payload = encode_payload(result)
        except _WorkflowSuspended:
            outcome = await self._submit_workflow_schedule(
                run_id=str(task_payload["run_id"]),
                workflow_task_id=str(task_payload["workflow_task_id"]),
                attempt_no=int(task_payload["attempt_no"]),
                command=controller.pending_command,
            )
            return outcome in _WORKFLOW_ACK_OUTCOMES
        except BaseException as exc:
            failure_payload = _safe_failure_payload(exc)
            outcome = await self._submit_workflow_failure(
                run_id=str(task_payload["run_id"]),
                workflow_task_id=str(task_payload["workflow_task_id"]),
                attempt_no=int(task_payload["attempt_no"]),
                error_payload=failure_payload,
            )
            return outcome in _WORKFLOW_ACK_OUTCOMES

        outcome = await self._submit_workflow_completion(
            run_id=str(task_payload["run_id"]),
            workflow_task_id=str(task_payload["workflow_task_id"]),
            attempt_no=int(task_payload["attempt_no"]),
            result_payload=result_payload,
        )
        return outcome in _WORKFLOW_ACK_OUTCOMES

    async def _process_activity_task(self, task_payload: dict[str, Any]) -> bool:
        registration = self._activity_by_name.get(str(task_payload["activity_name"]))
        activity_execution_id = str(task_payload["activity_execution_id"])
        attempt_no = int(task_payload["attempt_no"])
        if registration is None:
            payload = _safe_failure_payload(
                RemoteActivityError(
                    f"Worker for task queue {self.task_queue!r} cannot execute activity {task_payload['activity_name']!r}."
                )
            )
            outcome = await self._submit_activity_completion(
                activity_execution_id=activity_execution_id,
                attempt_no=attempt_no,
                status="failed",
                payload=payload,
            )
            return outcome in _ACTIVITY_ACK_OUTCOMES

        try:
            with activity._activate_execution_context(
                activity.ActivityExecutionInfo(
                    activity_execution_id=activity_execution_id,
                    attempt_no=attempt_no,
                    activity_name=str(task_payload["activity_name"]),
                    task_queue=str(task_payload["task_queue"]),
                    workflow_id=str(task_payload["workflow_id"]),
                    run_id=str(task_payload["run_id"]),
                )
            ):
                args = decode_args_payload(task_payload.get("input_payload"), registration.fn)
                result = registration.fn(*args)
                if inspect.isawaitable(result):
                    result = await result
        except BaseException as exc:
            payload = _safe_failure_payload(exc)
            status = "failed"
        else:
            payload = encode_payload(result)
            status = "completed"

        outcome = await self._submit_activity_completion(
            activity_execution_id=activity_execution_id,
            attempt_no=attempt_no,
            status=status,
            payload=payload,
        )
        return outcome in _ACTIVITY_ACK_OUTCOMES

    async def _fetch_run_history(self, run_id: str) -> list[dict[str, Any]]:
        assert self._http_client is not None
        response = await self._http_client.get(f"/runs/{run_id}/history")
        response.raise_for_status()
        body = response.json()
        return [_restore_history_event(event) for event in body["events"]]

    async def _submit_workflow_schedule(
        self,
        *,
        run_id: str,
        workflow_task_id: str,
        attempt_no: int,
        command: _ActivityScheduleCommand,
    ) -> str:
        assert self._http_client is not None
        retry_policy = _retry_policy_payload(command.retry_policy)
        response = await self._http_client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task_id,
                "attempt_no": attempt_no,
                "command": {
                    "kind": "schedule_activity",
                    "activity_name": command.activity_name,
                    "activity_task_queue": command.task_queue,
                    "input_payload_b64": to_base64(command.input_payload),
                    "retry_policy": retry_policy,
                    "schedule_to_close_timeout_ms": command.schedule_to_close_timeout_ms,
                },
            },
        )
        response.raise_for_status()
        return response.json()["outcome"]

    async def _submit_workflow_completion(
        self,
        *,
        run_id: str,
        workflow_task_id: str,
        attempt_no: int,
        result_payload: bytes,
    ) -> str:
        assert self._http_client is not None
        response = await self._http_client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task_id,
                "attempt_no": attempt_no,
                "command": {
                    "kind": "complete_workflow",
                    "result_payload_b64": to_base64(result_payload),
                },
            },
        )
        response.raise_for_status()
        return response.json()["outcome"]

    async def _submit_workflow_failure(
        self,
        *,
        run_id: str,
        workflow_task_id: str,
        attempt_no: int,
        error_payload: bytes,
    ) -> str:
        assert self._http_client is not None
        response = await self._http_client.post(
            "/workflow-tasks/complete",
            json={
                "run_id": run_id,
                "workflow_task_id": workflow_task_id,
                "attempt_no": attempt_no,
                "command": {
                    "kind": "fail_workflow",
                    "error_payload_b64": to_base64(error_payload),
                },
            },
        )
        response.raise_for_status()
        return response.json()["outcome"]

    async def _submit_activity_completion(
        self,
        *,
        activity_execution_id: str,
        attempt_no: int,
        status: str,
        payload: bytes,
    ) -> str:
        assert self._http_client is not None
        response = await self._http_client.post(
            "/activity-tasks/complete",
            json={
                "activity_execution_id": activity_execution_id,
                "attempt_no": attempt_no,
                "status": status,
                "result_payload_b64": to_base64(payload) if status == "completed" else None,
                "error_payload_b64": to_base64(payload) if status == "failed" else None,
            },
        )
        response.raise_for_status()
        return response.json()["outcome"]


@dataclass(frozen=True, slots=True)
class _ActivityScheduleCommand:
    activity_name: str
    task_queue: str
    input_payload: bytes
    retry_policy: RetryPolicy | None
    schedule_to_close_timeout_ms: int | None


class _WorkflowSuspended(Exception):
    """Internal control-flow exception for deterministic workflow scheduling."""


class _WorkflowReplayController:
    def __init__(
        self,
        history: list[dict[str, Any]],
        *,
        task_queue: str,
    ) -> None:
        if not history or history[0].get("event_type") != "WorkflowStarted":
            raise NonDeterministicWorkflowError(
                "Workflow history must begin with WorkflowStarted."
            )
        self._history = history
        self._task_queue = task_queue
        self._index = 1
        self.workflow_input_payload = history[0].get("input_payload")
        self.pending_command: _ActivityScheduleCommand | None = None

    async def execute_activity(
        self,
        activity_name: str,
        args: Sequence[Any],
        options: ActivityOptions,
    ) -> Any:
        if self.pending_command is not None:
            raise NonDeterministicWorkflowError(
                "Workflow attempted to schedule multiple activities in a single task."
            )

        if self._index >= len(self._history):
            self.pending_command = _ActivityScheduleCommand(
                activity_name=activity_name,
                task_queue=options.task_queue or self._task_queue,
                input_payload=encode_args_payload(tuple(args)),
                retry_policy=options.retry_policy,
                schedule_to_close_timeout_ms=_timedelta_to_ms(
                    options.schedule_to_close_timeout
                ),
            )
            raise _WorkflowSuspended()

        event = self._history[self._index]
        if event.get("event_type") != "ActivityScheduled":
            raise NonDeterministicWorkflowError(
                f"Expected ActivityScheduled, found {event.get('event_type')!r}."
            )

        expected_payload = encode_args_payload(tuple(args))
        expected_queue = options.task_queue or self._task_queue
        expected_timeout_ms = _timedelta_to_ms(options.schedule_to_close_timeout) or 0
        expected_max_attempts = (
            options.retry_policy.max_attempts if options.retry_policy else 1
        )
        if event.get("activity_name") != activity_name:
            raise NonDeterministicWorkflowError(
                f"Expected scheduled activity {event.get('activity_name')!r}, got {activity_name!r}."
            )
        if event.get("task_queue") != expected_queue:
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} used task queue {expected_queue!r}, "
                f"but history recorded {event.get('task_queue')!r}."
            )
        if event.get("input_payload") != expected_payload:
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} input payload does not match stored history."
            )
        if int(event.get("schedule_to_close_timeout_ms") or 0) != expected_timeout_ms:
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} timeout does not match stored history."
            )
        if int(event.get("max_attempts") or 1) != int(expected_max_attempts or 1):
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} retry policy does not match stored history."
            )

        activity_execution_id = str(event["activity_execution_id"])
        self._index += 1
        while self._index < len(self._history):
            outcome = self._history[self._index]
            if outcome.get("activity_execution_id") != activity_execution_id:
                break
            event_type = outcome.get("event_type")
            if event_type in _ATTEMPT_EVENT_TYPES:
                self._index += 1
                continue
            if event_type == "ActivityCompleted":
                self._index += 1
                return decode_payload(outcome.get("result_payload"))
            if event_type == "ActivityFailed":
                self._index += 1
                raise decode_failure_payload(
                    outcome.get("error_payload"),
                    fallback_error=RemoteActivityError,
                )
            break

        raise NonDeterministicWorkflowError(
            f"Activity {activity_name!r} has no terminal outcome in history."
        )

    def assert_fully_replayed(self) -> None:
        if self.pending_command is not None:
            return
        if self._index != len(self._history):
            raise NonDeterministicWorkflowError(
                "Workflow completed without consuming the full stored history."
            )


async def _read_stream_payload(
    store: FluxiRedisStore,
    *,
    stream_key: str,
    group_name: str,
    consumer_name: str,
) -> tuple[str, dict[str, Any]] | None:
    messages = await store.redis.xreadgroup(
        group_name,
        consumer_name,
        {stream_key: ">"},
        count=1,
        block=250,
    )
    if not messages:
        return None
    _, stream_messages = messages[0]
    if not stream_messages:
        return None
    message_id, values = stream_messages[0]
    payload = values[b"payload"] if b"payload" in values else values["payload"]
    decoded_id = message_id.decode("utf-8") if isinstance(message_id, bytes) else message_id
    return decoded_id, unpackb(payload)


def _register_workflows(
    workflows: Sequence[type[Any]],
) -> tuple[WorkflowRegistration, ...]:
    registry = workflow.WorkflowRegistry()
    return tuple(registry.register(workflow_cls) for workflow_cls in workflows)


def _register_activities(
    activities: Sequence[Callable[..., Any]],
) -> tuple[ActivityRegistration, ...]:
    registry = activity.ActivityRegistry()
    return tuple(registry.register(fn) for fn in activities)


def _restore_history_event(raw_event: dict[str, Any]) -> dict[str, Any]:
    restored: dict[str, Any] = {}
    for key, value in raw_event.items():
        if key.endswith("_payload") and isinstance(value, str):
            restored[key] = from_base64(value)
        else:
            restored[key] = value
    return restored


def _timedelta_to_ms(value: timedelta | None) -> int | None:
    if value is None:
        return None
    return int(value.total_seconds() * 1000)


def _retry_policy_payload(retry_policy: RetryPolicy | None) -> dict[str, Any] | None:
    if retry_policy is None:
        return None
    return {
        "max_attempts": retry_policy.max_attempts,
        "initial_interval_ms": (
            int(retry_policy.initial_interval_seconds * 1000)
            if retry_policy.initial_interval_seconds is not None
            else None
        ),
        "backoff_coefficient": retry_policy.backoff_coefficient,
        "max_interval_ms": (
            int(retry_policy.max_interval_seconds * 1000)
            if retry_policy.max_interval_seconds is not None
            else None
        ),
    }


def _safe_failure_payload(exc: BaseException) -> bytes:
    try:
        return encode_failure_payload(exc)
    except Exception as codec_error:
        fallback = RemoteWorkflowError(
            f"Failed to encode remote exception {exc.__class__.__name__}: {codec_error}"
        )
        return encode_failure_payload(fallback)


def _decode_workflow_result(
    payload_b64: str | None,
    run_callable: Callable[..., Any] | None,
) -> Any:
    payload = from_base64(payload_b64)
    if run_callable is None:
        return decode_payload(payload)
    return decode_return_payload(payload, run_callable)


def _decode_workflow_failure(payload_b64: str | None) -> BaseException:
    return decode_failure_payload(
        from_base64(payload_b64),
        fallback_error=RemoteWorkflowError,
    )


def _resolve_terminal_start_response(
    body: dict[str, Any],
    *,
    workflow_id: str,
) -> dict[str, Any] | object:
    if body["decision"] == "duplicate_rejected":
        raise WorkflowAlreadyStartedError(
            f"Workflow id {workflow_id!r} is already in use."
        )
    if body["status"] in {"completed", "failed"}:
        return body
    return _PENDING_RESULT


def _should_retry_start_request(
    start_policy: StartPolicy,
    status_code: int | None,
) -> bool:
    if start_policy == StartPolicy.ALLOW_DUPLICATE:
        return False
    if status_code is None:
        return True
    return status_code in _RESULT_RETRYABLE_HTTP_STATUS_CODES


def _is_missing_stream_group_error(exc: ResponseError) -> bool:
    return "NOGROUP" in str(exc).upper()
