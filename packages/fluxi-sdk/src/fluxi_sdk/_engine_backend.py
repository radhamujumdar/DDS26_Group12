"""Engine-backed SDK runtime using fluxi-server plus Redis Streams."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
import inspect
import logging
import os
import socket
import time
from typing import Any, Callable
import uuid

import httpx
from redis.asyncio.sentinel import MasterNotFoundError
from redis.exceptions import ConnectionError, ReadOnlyError, ResponseError, TimeoutError

from fluxi_engine.codecs import from_base64, to_base64, unpackb
from fluxi_engine.config import FluxiSettings, create_redis_client
from fluxi_engine.keys import activity_queue, workflow_queue
from fluxi_engine.models import RetryPolicyConfig, WorkflowTaskCommand
from fluxi_engine.observability import (
    elapsed_ms,
    redis_stream_message_age_ms,
    trace_logging_enabled,
)
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
from ._workflow_session import WorkflowSession, build_session_from_history

_WORKFLOW_ACK_OUTCOMES = {"scheduled_activity", "waiting", "completed", "stale", "conflict", "missing"}
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
TRACE_LOGGING_ENABLED = trace_logging_enabled()


class EngineWorkflowBackend:
    def __init__(self, config: EngineConnectionConfig) -> None:
        self._config = config
        self._control_http_client: httpx.AsyncClient | None = None
        self._result_http_client: httpx.AsyncClient | None = None
        self._control_client_lock: asyncio.Lock | None = None
        self._result_client_lock: asyncio.Lock | None = None

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
        execute_start = time.perf_counter()

        if TRACE_LOGGING_ENABLED:
            logger.info(
                "workflow.client.execute.start workflow_id=%s workflow_name=%s task_queue=%s start_policy=%s arg_count=%d",
                workflow_id,
                workflow_name,
                task_queue,
                start_policy.value,
                len(args),
            )

        control_client = await self._get_control_http_client()
        start_request = {
            "workflow_id": workflow_id,
            "workflow_name": workflow_name,
            "task_queue": task_queue,
            "start_policy": start_policy.value,
            "input_payload_b64": to_base64(input_payload),
        }
        start_or_attach_start = time.perf_counter()
        body = await self._start_or_attach(
            control_client,
            request=start_request,
            start_policy=start_policy,
        )
        if TRACE_LOGGING_ENABLED:
            logger.info(
                "workflow.client.execute.start_response workflow_id=%s decision=%s status=%s run_id=%s run_no=%s duration_ms=%.2f",
                workflow_id,
                body.get("decision"),
                body.get("status"),
                body.get("run_id"),
                body.get("run_no"),
                elapsed_ms(start_or_attach_start),
            )
        terminal = _resolve_terminal_start_response(
            body,
            workflow_id=workflow_id,
        )
        if terminal is not _PENDING_RESULT:
            if terminal["status"] == "completed":
                if TRACE_LOGGING_ENABLED:
                    logger.info(
                        "workflow.client.execute.done workflow_id=%s status=completed duration_ms=%.2f",
                        workflow_id,
                        elapsed_ms(execute_start),
                    )
                return _decode_workflow_result(
                    terminal.get("result_payload_b64"),
                    run_callable,
                )
            if terminal["status"] == "failed":
                if TRACE_LOGGING_ENABLED:
                    logger.info(
                        "workflow.client.execute.done workflow_id=%s status=failed duration_ms=%.2f",
                        workflow_id,
                        elapsed_ms(execute_start),
                    )
                raise _decode_workflow_failure(terminal.get("error_payload_b64"))

        while True:
            poll_start = time.perf_counter()
            snapshot = await self._poll_workflow_result(
                await self._get_result_http_client(),
                workflow_id=workflow_id,
                start_policy=start_policy,
                start_request=start_request,
                control_client=control_client,
            )
            if snapshot is _PENDING_RESULT:
                if TRACE_LOGGING_ENABLED:
                    logger.info(
                        "workflow.client.execute.pending workflow_id=%s wait_cycle_ms=%.2f total_duration_ms=%.2f",
                        workflow_id,
                        elapsed_ms(poll_start),
                        elapsed_ms(execute_start),
                    )
                continue
            if snapshot["status"] == "completed":
                if TRACE_LOGGING_ENABLED:
                    logger.info(
                        "workflow.client.execute.done workflow_id=%s status=completed duration_ms=%.2f",
                        workflow_id,
                        elapsed_ms(execute_start),
                    )
                return _decode_workflow_result(
                    snapshot.get("result_payload_b64"),
                    run_callable,
                )
            if snapshot["status"] == "failed":
                if TRACE_LOGGING_ENABLED:
                    logger.info(
                        "workflow.client.execute.done workflow_id=%s status=failed duration_ms=%.2f",
                        workflow_id,
                        elapsed_ms(execute_start),
                    )
                raise _decode_workflow_failure(snapshot.get("error_payload_b64"))

    async def aclose(self) -> None:
        control_client = self._control_http_client
        result_client = self._result_http_client
        self._control_http_client = None
        self._result_http_client = None
        clients = []
        if control_client is not None and not control_client.is_closed:
            clients.append(control_client.aclose())
        if result_client is not None and not result_client.is_closed:
            clients.append(result_client.aclose())
        if clients:
            await asyncio.gather(*clients)

    async def _get_control_http_client(self) -> httpx.AsyncClient:
        client = self._control_http_client
        if client is not None and not client.is_closed:
            return client
        if self._control_client_lock is None:
            self._control_client_lock = asyncio.Lock()
        async with self._control_client_lock:
            client = self._control_http_client
            if client is not None and not client.is_closed:
                return client
            client = _build_http_client(
                self._config,
                max_connections=self._config.http_control_max_connections,
                max_keepalive_connections=self._config.http_control_max_keepalive_connections,
            )
            self._control_http_client = client
            return client

    async def _get_result_http_client(self) -> httpx.AsyncClient:
        client = self._result_http_client
        if client is not None and not client.is_closed:
            return client
        if self._result_client_lock is None:
            self._result_client_lock = asyncio.Lock()
        async with self._result_client_lock:
            client = self._result_http_client
            if client is not None and not client.is_closed:
                return client
            client = _build_http_client(
                self._config,
                max_connections=self._config.http_result_max_connections,
                max_keepalive_connections=self._config.http_result_max_keepalive_connections,
            )
            self._result_http_client = client
            return client

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
        control_client: httpx.AsyncClient,
    ) -> dict[str, Any] | object:
        while True:
            try:
                result_response = await client.get(
                    f"/workflows/{workflow_id}/result",
                    params={"wait_ms": self._config.result_wait_timeout_ms},
                )
                if result_response.status_code == 404 and _should_retry_start_request(
                    start_policy,
                    404,
                ):
                    body = await self._start_or_attach(
                        control_client,
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
        max_concurrent_workflow_tasks: int,
        max_concurrent_activity_tasks: int,
    ) -> _EngineWorkerBinding:
        workflow_registrations = _register_workflows(workflows)
        activity_registrations = _register_activities(activities)
        return _EngineWorkerBinding(
            config=self._config,
            task_queue=task_queue,
            workflows=workflow_registrations,
            activities=activity_registrations,
            max_concurrent_workflow_tasks=max_concurrent_workflow_tasks,
            max_concurrent_activity_tasks=max_concurrent_activity_tasks,
        )


@dataclass(slots=True)
class _WorkflowSessionCacheEntry:
    session: WorkflowSession
    updated_at_monotonic: float


@dataclass(slots=True)
class _EngineWorkerBinding:
    config: EngineConnectionConfig
    task_queue: str
    workflows: tuple[WorkflowRegistration, ...]
    activities: tuple[ActivityRegistration, ...]
    max_concurrent_workflow_tasks: int
    max_concurrent_activity_tasks: int
    _settings: FluxiSettings = field(init=False, repr=False)
    _workflow_by_name: dict[str, WorkflowRegistration] = field(init=False, repr=False)
    _activity_by_name: dict[str, ActivityRegistration] = field(init=False, repr=False)
    _consumer_name: str = field(init=False, repr=False)
    _stop_event: asyncio.Event = field(init=False, repr=False)
    _stopped_event: asyncio.Event = field(init=False, repr=False)
    _workflow_capacity_event: asyncio.Event = field(init=False, repr=False)
    _activity_capacity_event: asyncio.Event = field(init=False, repr=False)
    _sticky_task_queue: str = field(init=False, repr=False)
    _redis_store: FluxiRedisStore | None = field(init=False, default=None, repr=False)
    _poller_tasks: list[asyncio.Task[None]] = field(init=False, default_factory=list, repr=False)
    _workflow_inflight: set[asyncio.Task[None]] = field(init=False, default_factory=set, repr=False)
    _activity_inflight: set[asyncio.Task[None]] = field(init=False, default_factory=set, repr=False)
    _workflow_sessions: OrderedDict[str, _WorkflowSessionCacheEntry] = field(
        init=False,
        default_factory=OrderedDict,
        repr=False,
    )
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
            result_wait_timeout_ms=self.config.result_wait_timeout_ms,
            result_poll_interval_ms=self.config.result_poll_interval_ms,
            sticky_schedule_to_start_timeout_ms=self.config.sticky_schedule_to_start_timeout_ms,
            sticky_cache_max_runs=self.config.sticky_cache_max_runs,
            sticky_cache_ttl_ms=self.config.sticky_cache_ttl_ms,
        )
        self._workflow_by_name = {registration.name: registration for registration in self.workflows}
        self._activity_by_name = {registration.name: registration for registration in self.activities}
        self._consumer_name = (
            f"fluxi-sdk:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        )
        self._sticky_task_queue = f"{self.task_queue}::sticky::{self._consumer_name}"
        self._stop_event = asyncio.Event()
        self._stopped_event = asyncio.Event()
        self._stopped_event.set()
        self._workflow_capacity_event = asyncio.Event()
        self._workflow_capacity_event.set()
        self._activity_capacity_event = asyncio.Event()
        self._activity_capacity_event.set()
        self._redis_store: FluxiRedisStore | None = None
        self._poller_tasks = []
        self._workflow_inflight = set()
        self._activity_inflight = set()
        self._workflow_sessions = OrderedDict()
        self._running = False

    async def start(self) -> None:
        if self._running:
            return

        self._stop_event = asyncio.Event()
        self._stopped_event = asyncio.Event()
        self._workflow_capacity_event = asyncio.Event()
        self._workflow_capacity_event.set()
        self._activity_capacity_event = asyncio.Event()
        self._activity_capacity_event.set()
        self._poller_tasks = []
        self._workflow_inflight = set()
        self._activity_inflight = set()
        self._workflow_sessions = OrderedDict()
        redis = create_redis_client(self._settings)
        self._redis_store = FluxiRedisStore(redis, self._settings)

        if self.workflows:
            await self._redis_store.ensure_workflow_queue(self.task_queue)
            await self._redis_store.ensure_workflow_queue(self._sticky_task_queue)
            self._poller_tasks.append(
                asyncio.create_task(
                    self._workflow_loop(workflow_queue(self._settings.key_prefix, self.task_queue)),
                    name=f"fluxi-sdk:workflow:{self.task_queue}:{self._consumer_name}",
                )
            )
            self._poller_tasks.append(
                asyncio.create_task(
                    self._workflow_loop(
                        workflow_queue(self._settings.key_prefix, self._sticky_task_queue)
                    ),
                    name=f"fluxi-sdk:workflow:{self._sticky_task_queue}:{self._consumer_name}",
                )
            )
        if self.activities:
            await self._redis_store.ensure_activity_queue(self.task_queue)
            self._poller_tasks.append(
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
        tasks, self._poller_tasks = self._poller_tasks, []
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._drain_inflight_tasks()
        await self._clear_workflow_sessions()
        if self._redis_store is not None:
            await self._redis_store.aclose()
        self._redis_store = None
        self._running = False
        self._stopped_event.set()

    async def _workflow_loop(self, stream_key: str) -> None:
        assert self._redis_store is not None
        try:
            while not self._stop_event.is_set():
                try:
                    await self._wait_for_capacity(
                        self._workflow_inflight,
                        self.max_concurrent_workflow_tasks,
                        self._workflow_capacity_event,
                    )
                    if self._stop_event.is_set():
                        continue
                    available = self.max_concurrent_workflow_tasks - len(
                        self._workflow_inflight
                    )
                    if available <= 0:
                        continue
                    payloads = await _read_stream_payloads(
                        self._redis_store,
                        stream_key=stream_key,
                        group_name=self._settings.workflow_consumer_group,
                        consumer_name=self._consumer_name,
                        count=available,
                    )
                    if not payloads:
                        continue
                    for message_id, task_payload in payloads:
                        self._track_task(
                            asyncio.create_task(
                                self._handle_workflow_message(
                                    stream_key=stream_key,
                                    message_id=message_id,
                                    task_payload=task_payload,
                                ),
                                name=(
                                    f"fluxi-sdk:workflow-task:"
                                    f"{self.task_queue}:{task_payload.get('workflow_task_id')}"
                                ),
                            ),
                            self._workflow_inflight,
                            self._workflow_capacity_event,
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
                    await self._wait_for_capacity(
                        self._activity_inflight,
                        self.max_concurrent_activity_tasks,
                        self._activity_capacity_event,
                    )
                    if self._stop_event.is_set():
                        continue
                    available = self.max_concurrent_activity_tasks - len(
                        self._activity_inflight
                    )
                    if available <= 0:
                        continue
                    payloads = await _read_stream_payloads(
                        self._redis_store,
                        stream_key=stream_key,
                        group_name=self._settings.activity_consumer_group,
                        consumer_name=self._consumer_name,
                        count=available,
                    )
                    if not payloads:
                        continue
                    for message_id, task_payload in payloads:
                        self._track_task(
                            asyncio.create_task(
                                self._handle_activity_message(
                                    stream_key=stream_key,
                                    message_id=message_id,
                                    task_payload=task_payload,
                                ),
                                name=(
                                    f"fluxi-sdk:activity-task:"
                                    f"{self.task_queue}:{task_payload.get('activity_execution_id')}"
                                ),
                            ),
                            self._activity_inflight,
                            self._activity_capacity_event,
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
        await self._redis_store.ensure_workflow_queue(self._sticky_task_queue)

    async def _ensure_activity_group(self) -> None:
        assert self._redis_store is not None
        await self._redis_store.ensure_activity_queue(self.task_queue)

    async def _handle_workflow_message(
        self,
        *,
        stream_key: str,
        message_id: str,
        task_payload: dict[str, Any],
    ) -> None:
        handle_start = time.perf_counter()
        workflow_task_id = str(task_payload.get("workflow_task_id"))
        run_id = str(task_payload.get("run_id"))
        queue_wait_ms = redis_stream_message_age_ms(message_id)
        if TRACE_LOGGING_ENABLED:
            logger.info(
                "workflow.task.received task_queue=%s stream=%s run_id=%s workflow_task_id=%s attempt_no=%s queue_wait_ms=%s",
                self.task_queue,
                stream_key,
                run_id,
                workflow_task_id,
                task_payload.get("attempt_no"),
                queue_wait_ms,
            )
        try:
            should_ack = await self._process_workflow_task(task_payload)
            if should_ack:
                assert self._redis_store is not None
                await self._redis_store.redis.xack(
                    stream_key,
                    self._settings.workflow_consumer_group,
                    message_id,
                )
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "workflow.task.handled task_queue=%s run_id=%s workflow_task_id=%s should_ack=%s total_handle_ms=%.2f",
                    self.task_queue,
                    run_id,
                    workflow_task_id,
                    should_ack,
                    elapsed_ms(handle_start),
                )
        except asyncio.CancelledError:
            raise
        except _REDIS_RECOVERABLE_ERRORS as exc:
            logger.warning(
                "Transient Redis/Sentinel error while processing workflow task %s on %s: %s",
                task_payload.get("workflow_task_id"),
                self.task_queue,
                exc,
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "Transient HTTP error while processing workflow task %s on %s: %s",
                task_payload.get("workflow_task_id"),
                self.task_queue,
                exc,
            )
        except Exception:
            logger.exception(
                "Unexpected error while processing workflow task %s on %s.",
                task_payload.get("workflow_task_id"),
                self.task_queue,
            )

    async def _handle_activity_message(
        self,
        *,
        stream_key: str,
        message_id: str,
        task_payload: dict[str, Any],
    ) -> None:
        handle_start = time.perf_counter()
        activity_execution_id = str(task_payload.get("activity_execution_id"))
        run_id = str(task_payload.get("run_id"))
        queue_wait_ms = redis_stream_message_age_ms(message_id)
        if TRACE_LOGGING_ENABLED:
            logger.info(
                "activity.task.received task_queue=%s stream=%s run_id=%s activity_execution_id=%s attempt_no=%s queue_wait_ms=%s",
                self.task_queue,
                stream_key,
                run_id,
                activity_execution_id,
                task_payload.get("attempt_no"),
                queue_wait_ms,
            )
        try:
            should_ack = await self._process_activity_task(task_payload)
            if should_ack:
                assert self._redis_store is not None
                await self._redis_store.redis.xack(
                    stream_key,
                    self._settings.activity_consumer_group,
                    message_id,
                )
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "activity.task.handled task_queue=%s run_id=%s activity_execution_id=%s should_ack=%s total_handle_ms=%.2f",
                    self.task_queue,
                    run_id,
                    activity_execution_id,
                    should_ack,
                    elapsed_ms(handle_start),
                )
        except asyncio.CancelledError:
            raise
        except _REDIS_RECOVERABLE_ERRORS as exc:
            logger.warning(
                "Transient Redis/Sentinel error while processing activity %s on %s: %s",
                task_payload.get("activity_execution_id"),
                self.task_queue,
                exc,
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "Transient HTTP error while processing activity %s on %s: %s",
                task_payload.get("activity_execution_id"),
                self.task_queue,
                exc,
            )
        except Exception:
            logger.exception(
                "Unexpected error while processing activity %s on %s.",
                task_payload.get("activity_execution_id"),
                self.task_queue,
            )

    async def _drain_inflight_tasks(self, grace_seconds: float = 5.0) -> None:
        inflight = [*self._workflow_inflight, *self._activity_inflight]
        if not inflight:
            return
        done, pending = await asyncio.wait(inflight, timeout=grace_seconds)
        if pending:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if done:
            await asyncio.gather(*done, return_exceptions=True)

    async def _wait_for_capacity(
        self,
        inflight: set[asyncio.Task[None]],
        max_concurrency: int,
        capacity_event: asyncio.Event,
    ) -> None:
        while len(inflight) >= max_concurrency and not self._stop_event.is_set():
            capacity_event.clear()
            if len(inflight) < max_concurrency:
                capacity_event.set()
                return
            stop_wait = asyncio.create_task(self._stop_event.wait())
            release_wait = asyncio.create_task(capacity_event.wait())
            done, pending = await asyncio.wait(
                {stop_wait, release_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                await asyncio.gather(task, return_exceptions=True)

    def _track_task(
        self,
        task: asyncio.Task[None],
        inflight: set[asyncio.Task[None]],
        capacity_event: asyncio.Event,
    ) -> None:
        inflight.add(task)

        def _done(completed: asyncio.Task[None]) -> None:
            inflight.discard(completed)
            capacity_event.set()

        task.add_done_callback(_done)

    async def _process_workflow_task(self, task_payload: dict[str, Any]) -> bool:
        run_id = str(task_payload["run_id"])
        workflow_name = str(task_payload["workflow_name"])
        workflow_task_id = str(task_payload["workflow_task_id"])
        attempt_no = int(task_payload["attempt_no"])
        processing_start = time.perf_counter()
        registration = self._workflow_by_name.get(workflow_name)
        if registration is None:
            failure_payload = _safe_failure_payload(
                RemoteWorkflowError(
                    f"Worker for task queue {self.task_queue!r} cannot execute workflow {task_payload['workflow_name']!r}."
                )
            )
            submit_start = time.perf_counter()
            completion = await self._submit_workflow_commands(
                run_id=run_id,
                workflow_task_id=workflow_task_id,
                attempt_no=attempt_no,
                commands=[{"kind": "fail_workflow", "error_payload": failure_payload}],
            )
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "workflow.task.registration_missing task_queue=%s run_id=%s workflow_task_id=%s workflow_name=%s submit_ms=%.2f total_ms=%.2f outcome=%s",
                    self.task_queue,
                    run_id,
                    workflow_task_id,
                    workflow_name,
                    elapsed_ms(submit_start),
                    elapsed_ms(processing_start),
                    completion.outcome,
                )
            return completion.outcome in _WORKFLOW_ACK_OUTCOMES

        if attempt_no > 1:
            await self._evict_workflow_session(
                run_id,
                reason="workflow_task_retry",
            )

        session = None
        cache_hit = False
        history_event_count = 0
        history_fetch_ms = 0.0
        history_apply_ms = 0.0
        rebuild_reason = "cache_miss"

        try:
            session_lookup_start = time.perf_counter()
            session = await self._get_cached_workflow_session(run_id)
            if session is None:
                history_fetch_start = time.perf_counter()
                history, history_index = await self._fetch_history_events(run_id)
                history_fetch_ms = elapsed_ms(history_fetch_start)
                history_event_count = len(history)
                rebuild_start = time.perf_counter()
                session = build_session_from_history(
                    run_id=run_id,
                    workflow_id=str(task_payload["workflow_id"]),
                    task_queue=self.task_queue,
                    history=history,
                    history_index=history_index,
                    registration=registration,
                    activity_by_name=self._activity_by_name,
                )
                history_apply_ms = elapsed_ms(rebuild_start)
                await self._remember_workflow_session(run_id, session)
            else:
                cache_hit = True
                rebuild_reason = "cache_hit"
                history_fetch_start = time.perf_counter()
                history, history_index = await self._fetch_history_events(
                    run_id,
                    after_index=session.last_history_index,
                )
                history_fetch_ms = elapsed_ms(history_fetch_start)
                history_event_count = len(history)
                apply_start = time.perf_counter()
                session.apply_history_events(history, next_index=history_index)
                history_apply_ms = elapsed_ms(apply_start)
                await self._remember_workflow_session(run_id, session)
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "workflow.session.%s task_queue=%s run_id=%s workflow_task_id=%s history_events=%d history_fetch_ms=%.2f history_apply_ms=%.2f lookup_ms=%.2f",
                    "hit" if cache_hit else "rebuild",
                    self.task_queue,
                    run_id,
                    workflow_task_id,
                    history_event_count,
                    history_fetch_ms,
                    history_apply_ms,
                    elapsed_ms(session_lookup_start),
                )

            drive_start = time.perf_counter()
            drive_result = await session.drive()
            drive_ms = elapsed_ms(drive_start)
            if (
                drive_result.status == "suspended"
                and not drive_result.commands
                and drive_result.waiting_remote_execution_id is not None
            ):
                for _ in range(2):
                    refresh_start = time.perf_counter()
                    refresh_events, refresh_index = await self._fetch_history_events(
                        run_id,
                        after_index=session.last_history_index,
                    )
                    refresh_fetch_ms = elapsed_ms(refresh_start)
                    if not refresh_events:
                        break
                    session.apply_history_events(refresh_events, next_index=refresh_index)
                    retry_drive_start = time.perf_counter()
                    drive_result = await session.drive()
                    drive_ms += elapsed_ms(retry_drive_start)
                    if TRACE_LOGGING_ENABLED:
                        logger.info(
                            "workflow.session.refresh task_queue=%s run_id=%s workflow_task_id=%s history_events=%d history_fetch_ms=%.2f waiting_activity=%s",
                            self.task_queue,
                            run_id,
                            workflow_task_id,
                            len(refresh_events),
                            refresh_fetch_ms,
                            drive_result.waiting_remote_execution_id,
                        )
                    if (
                        drive_result.status != "suspended"
                        or drive_result.commands
                        or drive_result.waiting_remote_execution_id is None
                    ):
                        break

            commands = list(drive_result.commands)
            if drive_result.status == "completed":
                commands.append(
                    {
                        "kind": "complete_workflow",
                        "result_payload": drive_result.result_payload,
                    }
                )
            elif drive_result.status == "failed":
                commands.append(
                    {
                        "kind": "fail_workflow",
                        "error_payload": drive_result.failure_payload,
                    }
                )

            submit_start = time.perf_counter()
            completion = await self._submit_workflow_commands(
                run_id=run_id,
                workflow_task_id=workflow_task_id,
                attempt_no=attempt_no,
                commands=commands,
            )
            submit_ms = elapsed_ms(submit_start)

            expected_execution_ids = tuple(
                str(command["activity_execution_id"])
                for command in commands
                if command["kind"] == "schedule_activity"
            )
            if expected_execution_ids and completion.activity_execution_ids:
                actual_execution_ids = tuple(completion.activity_execution_ids)
                if expected_execution_ids != actual_execution_ids:
                    logger.warning(
                        "workflow.task.execution_id_mismatch task_queue=%s run_id=%s workflow_task_id=%s expected=%s actual=%s",
                        self.task_queue,
                        run_id,
                        workflow_task_id,
                        expected_execution_ids,
                        actual_execution_ids,
                    )
                    await self._evict_workflow_session(
                        run_id,
                        reason="activity_execution_id_mismatch",
                    )

            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "workflow.task.%s task_queue=%s run_id=%s workflow_task_id=%s pending_commands=%d cache_hit=%s rebuild_reason=%s waiting_activity=%s drive_ms=%.2f submit_ms=%.2f total_ms=%.2f outcome=%s",
                    drive_result.status,
                    self.task_queue,
                    run_id,
                    workflow_task_id,
                    len(commands),
                    cache_hit,
                    rebuild_reason,
                    drive_result.waiting_remote_execution_id,
                    drive_ms,
                    submit_ms,
                    elapsed_ms(processing_start),
                    completion.outcome,
                )

            if drive_result.status in {"completed", "failed"} or completion.outcome in {
                "stale",
                "conflict",
                "missing",
            }:
                await self._evict_workflow_session(
                    run_id,
                    reason=drive_result.status,
                )
            elif completion.outcome in _WORKFLOW_ACK_OUTCOMES:
                await self._remember_workflow_session(run_id, session)
            else:
                await self._evict_workflow_session(
                    run_id,
                    reason=f"submit_outcome:{completion.outcome}",
                )
            return completion.outcome in _WORKFLOW_ACK_OUTCOMES
        except BaseException as exc:
            failure_payload = _safe_failure_payload(exc)
            submit_start = time.perf_counter()
            completion = await self._submit_workflow_commands(
                run_id=run_id,
                workflow_task_id=workflow_task_id,
                attempt_no=attempt_no,
                commands=[{"kind": "fail_workflow", "error_payload": failure_payload}],
            )
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "workflow.task.failed task_queue=%s run_id=%s workflow_task_id=%s cache_hit=%s history_events=%d error_type=%s submit_ms=%.2f total_ms=%.2f outcome=%s",
                    self.task_queue,
                    run_id,
                    workflow_task_id,
                    cache_hit,
                    history_event_count,
                    exc.__class__.__name__,
                    elapsed_ms(submit_start),
                    elapsed_ms(processing_start),
                    completion.outcome,
                )
            await self._evict_workflow_session(run_id, reason="workflow_failure")
            return completion.outcome in _WORKFLOW_ACK_OUTCOMES

    async def _process_activity_task(self, task_payload: dict[str, Any]) -> bool:
        activity_name = str(task_payload["activity_name"])
        activity_execution_id = str(task_payload["activity_execution_id"])
        attempt_no = int(task_payload["attempt_no"])
        run_id = str(task_payload["run_id"])
        processing_start = time.perf_counter()
        registration = self._activity_by_name.get(activity_name)
        if registration is None:
            payload = _safe_failure_payload(
                RemoteActivityError(
                    f"Worker for task queue {self.task_queue!r} cannot execute activity {task_payload['activity_name']!r}."
                )
            )
            submit_start = time.perf_counter()
            outcome = await self._submit_activity_completion(
                activity_execution_id=activity_execution_id,
                attempt_no=attempt_no,
                status="failed",
                payload=payload,
            )
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "activity.task.registration_missing task_queue=%s run_id=%s activity_execution_id=%s activity_name=%s submit_ms=%.2f total_ms=%.2f outcome=%s",
                    self.task_queue,
                    run_id,
                    activity_execution_id,
                    activity_name,
                    elapsed_ms(submit_start),
                    elapsed_ms(processing_start),
                    outcome,
                )
            return outcome in _ACTIVITY_ACK_OUTCOMES

        try:
            execution_start = time.perf_counter()
            with activity._activate_execution_context(
                activity.ActivityExecutionInfo(
                    activity_execution_id=activity_execution_id,
                    attempt_no=attempt_no,
                    activity_name=activity_name,
                    task_queue=str(task_payload["task_queue"]),
                    workflow_id=str(task_payload["workflow_id"]),
                    run_id=run_id,
                )
            ):
                args = decode_args_payload(task_payload.get("input_payload"), registration.fn)
                result = registration.fn(*args)
                if inspect.isawaitable(result):
                    result = await result
            execution_ms = elapsed_ms(execution_start)
        except BaseException as exc:
            payload = _safe_failure_payload(exc)
            status = "failed"
            execution_ms = elapsed_ms(execution_start)
            error_type = exc.__class__.__name__
        else:
            payload = encode_payload(result)
            status = "completed"
            error_type = ""

        submit_start = time.perf_counter()
        completion = await self._submit_activity_completion(
            activity_execution_id=activity_execution_id,
            attempt_no=attempt_no,
            status=status,
            payload=payload,
        )
        if TRACE_LOGGING_ENABLED:
            logger.info(
                "activity.task.finished task_queue=%s run_id=%s activity_execution_id=%s activity_name=%s status=%s error_type=%s execution_ms=%.2f submit_ms=%.2f total_ms=%.2f payload_bytes=%d outcome=%s",
                self.task_queue,
                run_id,
                activity_execution_id,
                activity_name,
                status,
                error_type,
                execution_ms,
                elapsed_ms(submit_start),
                elapsed_ms(processing_start),
                len(payload),
                completion.outcome,
            )
        return completion.outcome in _ACTIVITY_ACK_OUTCOMES

    async def _get_cached_workflow_session(self, run_id: str) -> WorkflowSession | None:
        cache_entry = self._workflow_sessions.get(run_id)
        if cache_entry is None:
            return None
        age_ms = (time.monotonic() - cache_entry.updated_at_monotonic) * 1000
        if age_ms > self._settings.sticky_cache_ttl_ms:
            await self._evict_workflow_session(run_id, reason="ttl_expired")
            return None
        self._workflow_sessions.move_to_end(run_id)
        return cache_entry.session

    async def _remember_workflow_session(
        self,
        run_id: str,
        session: WorkflowSession,
    ) -> None:
        self._workflow_sessions[run_id] = _WorkflowSessionCacheEntry(
            session=session,
            updated_at_monotonic=time.monotonic(),
        )
        self._workflow_sessions.move_to_end(run_id)
        while len(self._workflow_sessions) > self._settings.sticky_cache_max_runs:
            expired_run_id, expired_entry = self._workflow_sessions.popitem(last=False)
            if TRACE_LOGGING_ENABLED:
                logger.info(
                    "workflow.session.evict task_queue=%s run_id=%s reason=lru_evict",
                    self.task_queue,
                    expired_run_id,
                )
            await expired_entry.session.cancel()

    async def _evict_workflow_session(self, run_id: str, *, reason: str) -> None:
        cache_entry = self._workflow_sessions.pop(run_id, None)
        if cache_entry is None:
            return
        if TRACE_LOGGING_ENABLED:
            logger.info(
                "workflow.session.evict task_queue=%s run_id=%s reason=%s",
                self.task_queue,
                run_id,
                reason,
            )
        await cache_entry.session.cancel()

    async def _clear_workflow_sessions(self) -> None:
        run_ids = list(self._workflow_sessions.keys())
        for run_id in run_ids:
            await self._evict_workflow_session(run_id, reason="worker_shutdown")

    async def _fetch_history_events(
        self,
        run_id: str,
        *,
        after_index: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        assert self._redis_store is not None
        return await self._redis_store.get_history_page(run_id, after_index=after_index)

    async def _submit_workflow_commands(
        self,
        *,
        run_id: str,
        workflow_task_id: str,
        attempt_no: int,
        commands: list[dict[str, Any]],
    ):
        assert self._redis_store is not None
        sticky_expires_at_ms = int(time.time() * 1000) + self._settings.sticky_schedule_to_start_timeout_ms
        return await self._redis_store.complete_workflow_task(
            run_id=run_id,
            workflow_task_id=workflow_task_id,
            attempt_no=attempt_no,
            sticky_task_queue=self._sticky_task_queue,
            sticky_owner_id=self._consumer_name,
            sticky_expires_at_ms=sticky_expires_at_ms,
            commands=[_to_store_workflow_command(command) for command in commands],
        )

    async def _submit_activity_completion(
        self,
        *,
        activity_execution_id: str,
        attempt_no: int,
        status: str,
        payload: bytes,
    ):
        assert self._redis_store is not None
        return await self._redis_store.complete_activity_task(
            activity_execution_id=activity_execution_id,
            attempt_no=attempt_no,
            status=status,
            payload=payload,
        )


@dataclass(frozen=True, slots=True)
class _ActivityScheduleCommand:
    activity_name: str
    task_queue: str
    input_payload: bytes
    retry_policy: RetryPolicy | None
    schedule_to_close_timeout_ms: int | None


@dataclass(slots=True)
class _HistoryCacheEntry:
    history: list[dict[str, Any]]
    next_index: int
    updated_at_monotonic: float


def _serialize_workflow_commands(
    commands: Sequence[_ActivityScheduleCommand],
) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for command in commands:
        serialized.append(
            {
                "kind": "schedule_activity",
                "activity_name": command.activity_name,
                "activity_task_queue": command.task_queue,
                "input_payload_b64": to_base64(command.input_payload),
                "retry_policy": _retry_policy_payload(command.retry_policy),
                "schedule_to_close_timeout_ms": command.schedule_to_close_timeout_ms,
            }
        )
    return serialized


class _WorkflowSuspended(Exception):
    """Internal control-flow exception for deterministic workflow scheduling."""


@dataclass(frozen=True, slots=True)
class _ReplayActivityHandleState:
    activity_name: str
    pending: bool
    activity_execution_id: str | None = None
    terminal_outcome: dict[str, Any] | None = None


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
        self.workflow_input_payload = history[0].get("input_payload")
        self._schedule_events = [
            event for event in history if event.get("event_type") == "ActivityScheduled"
        ]
        self._schedule_index = 0
        self.pending_commands: list[_ActivityScheduleCommand] = []
        self._terminal_outcomes: dict[str, dict[str, Any]] = {}
        for event in history:
            event_type = event.get("event_type")
            if event_type in {"ActivityCompleted", "ActivityFailed"}:
                self._terminal_outcomes[str(event["activity_execution_id"])] = event

    def start_activity(
        self,
        activity_name: str,
        args: Sequence[Any],
        options: ActivityOptions,
    ) -> workflow.ActivityHandle[Any]:
        expected_payload = encode_args_payload(tuple(args))
        expected_queue = options.task_queue or self._task_queue
        expected_timeout_ms = _timedelta_to_ms(options.schedule_to_close_timeout)
        expected_max_attempts = (
            options.retry_policy.max_attempts if options.retry_policy else 1
        )
        expected_initial_interval_ms = (
            int(options.retry_policy.initial_interval_seconds * 1000)
            if options.retry_policy and options.retry_policy.initial_interval_seconds is not None
            else 0
        )
        expected_backoff_coefficient = (
            float(options.retry_policy.backoff_coefficient)
            if options.retry_policy and options.retry_policy.backoff_coefficient is not None
            else 0.0
        )
        expected_max_interval_ms = (
            int(options.retry_policy.max_interval_seconds * 1000)
            if options.retry_policy and options.retry_policy.max_interval_seconds is not None
            else 0
        )

        if self._schedule_index >= len(self._schedule_events):
            self.pending_commands.append(
                _ActivityScheduleCommand(
                    activity_name=activity_name,
                    task_queue=expected_queue,
                    input_payload=expected_payload,
                    retry_policy=options.retry_policy,
                    schedule_to_close_timeout_ms=expected_timeout_ms,
                )
            )
            return workflow.ActivityHandle(
                lambda: self._await_handle(
                    _ReplayActivityHandleState(
                        activity_name=activity_name,
                        pending=True,
                    )
                )
            )

        event = self._schedule_events[self._schedule_index]
        self._schedule_index += 1
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
        if int(event.get("schedule_to_close_timeout_ms") or 0) != int(expected_timeout_ms or 0):
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} timeout does not match stored history."
            )
        if int(event.get("max_attempts") or 1) != int(expected_max_attempts or 1):
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} retry policy does not match stored history."
            )
        if int(event.get("initial_interval_ms") or 0) != expected_initial_interval_ms:
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} retry policy does not match stored history."
            )
        if float(event.get("backoff_coefficient") or 0.0) != expected_backoff_coefficient:
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} retry policy does not match stored history."
            )
        if int(event.get("max_interval_ms") or 0) != expected_max_interval_ms:
            raise NonDeterministicWorkflowError(
                f"Activity {activity_name!r} retry policy does not match stored history."
            )

        activity_execution_id = str(event["activity_execution_id"])
        terminal_outcome = self._terminal_outcomes.get(activity_execution_id)
        return workflow.ActivityHandle(
            lambda: self._await_handle(
                _ReplayActivityHandleState(
                    activity_name=activity_name,
                    pending=False,
                    activity_execution_id=activity_execution_id,
                    terminal_outcome=terminal_outcome,
                )
            )
        )

    async def _await_handle(self, handle: _ReplayActivityHandleState) -> Any:
        if handle.pending or handle.terminal_outcome is None:
            raise _WorkflowSuspended()

        outcome = handle.terminal_outcome
        event_type = outcome.get("event_type")
        if event_type == "ActivityCompleted":
            return decode_payload(outcome.get("result_payload"))
        if event_type == "ActivityFailed":
            raise decode_failure_payload(
                outcome.get("error_payload"),
                fallback_error=RemoteActivityError,
            )
        raise NonDeterministicWorkflowError(
            f"Activity {handle.activity_name!r} has unsupported terminal outcome {event_type!r}."
        )

    def assert_fully_replayed(self) -> None:
        if self.pending_commands:
            raise NonDeterministicWorkflowError(
                "Workflow completed while new activity commands were still pending."
            )
        if self._schedule_index != len(self._schedule_events):
            raise NonDeterministicWorkflowError(
                "Workflow completed without consuming the full scheduled activity history."
            )


async def _read_stream_payloads(
    store: FluxiRedisStore,
    *,
    stream_key: str,
    group_name: str,
    consumer_name: str,
    count: int,
) -> list[tuple[str, dict[str, Any]]]:
    messages = await store.redis.xreadgroup(
        group_name,
        consumer_name,
        {stream_key: ">"},
        count=count,
        block=250,
    )
    if not messages:
        return []
    _, stream_messages = messages[0]
    if not stream_messages:
        return []
    decoded_messages: list[tuple[str, dict[str, Any]]] = []
    for message_id, values in stream_messages:
        payload = values[b"payload"] if b"payload" in values else values["payload"]
        decoded_id = (
            message_id.decode("utf-8")
            if isinstance(message_id, bytes)
            else message_id
        )
        decoded_messages.append((decoded_id, unpackb(payload)))
    return decoded_messages


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


def _to_store_workflow_command(command: dict[str, Any]) -> WorkflowTaskCommand:
    retry_policy = command.get("retry_policy")
    retry_policy_config = None
    if retry_policy is not None:
        retry_policy_config = RetryPolicyConfig(
            max_attempts=retry_policy.max_attempts,
            initial_interval_ms=(
                int(retry_policy.initial_interval_seconds * 1000)
                if retry_policy.initial_interval_seconds is not None
                else None
            ),
            backoff_coefficient=retry_policy.backoff_coefficient,
            max_interval_ms=(
                int(retry_policy.max_interval_seconds * 1000)
                if retry_policy.max_interval_seconds is not None
                else None
            ),
        )
    return WorkflowTaskCommand(
        kind=command["kind"],
        activity_execution_id=command.get("activity_execution_id"),
        activity_name=command.get("activity_name"),
        activity_task_queue=command.get("activity_task_queue"),
        activity_status=command.get("activity_status"),
        input_payload=command.get("input_payload"),
        retry_policy=retry_policy_config,
        schedule_to_close_timeout_ms=command.get("schedule_to_close_timeout_ms"),
        result_payload=command.get("result_payload"),
        error_payload=command.get("error_payload"),
    )


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


def _build_http_client(
    config: EngineConnectionConfig,
    *,
    max_connections: int | None = None,
    max_keepalive_connections: int | None = None,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=config.server_url.rstrip("/"),
        timeout=httpx.Timeout(
            connect=config.http_connect_timeout_seconds,
            read=config.http_read_timeout_seconds,
            write=config.http_write_timeout_seconds,
            pool=config.http_pool_timeout_seconds,
        ),
        limits=httpx.Limits(
            max_connections=max_connections or config.http_max_connections,
            max_keepalive_connections=(
                max_keepalive_connections or config.http_max_keepalive_connections
            ),
        ),
    )
