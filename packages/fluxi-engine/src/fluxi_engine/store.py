"""Redis-backed store and orchestration primitives for the Fluxi engine."""

from __future__ import annotations

from collections.abc import Iterable
import time
from typing import Any
import uuid

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .codecs import packb, unpackb
from .config import FluxiSettings, close_redis_client
from .keys import (
    activity_queue,
    activity_state,
    activity_task_queues,
    timers,
    workflow_control,
    workflow_history,
    workflow_queue,
    workflow_result_channel,
    workflow_state,
    workflow_task_queues,
)
from .models import (
    ActivityTaskCompletionResult,
    RetryPolicyConfig,
    StartWorkflowResult,
    WorkflowResultSnapshot,
    WorkflowTaskCommand,
    WorkflowTaskCompletionResult,
)
from .scripts import (
    APPLY_ACTIVITY_COMPLETION,
    APPLY_TIMER,
    APPLY_WORKFLOW_TASK_COMPLETION,
    START_OR_ATTACH_WORKFLOW,
)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _to_int(value: Any) -> int | None:
    text = _to_text(value)
    if text is None or text == "":
        return None
    return int(text)


def _to_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        if value == b"":
            return None
        return value
    if value == "":
        return None
    return str(value).encode("utf-8")


def _decode_hash(mapping: dict[Any, Any]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in mapping.items():
        decoded[_to_text(key) or ""] = value
    return decoded


class FluxiRedisStore:
    """Async Redis-backed storage and atomic operations for Fluxi."""

    def __init__(self, redis: Redis, settings: FluxiSettings) -> None:
        self.redis = redis
        self.settings = settings

    async def aclose(self) -> None:
        await close_redis_client(self.redis)

    async def flushdb(self) -> None:
        await self.redis.flushdb()

    async def ensure_workflow_queue(self, task_queue: str) -> None:
        await self._ensure_stream_group(
            stream_key=workflow_queue(self.settings.key_prefix, task_queue),
            group_name=self.settings.workflow_consumer_group,
            registry_key=workflow_task_queues(self.settings.key_prefix),
            task_queue=task_queue,
        )

    async def ensure_activity_queue(self, task_queue: str) -> None:
        await self._ensure_stream_group(
            stream_key=activity_queue(self.settings.key_prefix, task_queue),
            group_name=self.settings.activity_consumer_group,
            registry_key=activity_task_queues(self.settings.key_prefix),
            task_queue=task_queue,
        )

    async def _ensure_stream_group(
        self,
        *,
        stream_key: str,
        group_name: str,
        registry_key: str,
        task_queue: str,
    ) -> None:
        await self.redis.sadd(registry_key, task_queue)
        try:
            await self.redis.xgroup_create(
                name=stream_key,
                groupname=group_name,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def start_or_attach_workflow(
        self,
        *,
        workflow_id: str,
        workflow_name: str,
        task_queue: str,
        start_policy: str,
        input_payload: bytes | None,
    ) -> StartWorkflowResult:
        await self.ensure_workflow_queue(task_queue)

        run_id = f"run-{uuid.uuid4().hex}"
        raw = await self.redis.eval(
            START_OR_ATTACH_WORKFLOW,
            5,
            workflow_control(self.settings.key_prefix, workflow_id),
            workflow_state(self.settings.key_prefix, run_id),
            workflow_history(self.settings.key_prefix, run_id),
            workflow_queue(self.settings.key_prefix, task_queue),
            timers(self.settings.key_prefix),
            _now_ms(),
            workflow_id,
            workflow_name,
            task_queue,
            start_policy,
            input_payload or b"",
            run_id,
            self.settings.workflow_task_timeout_ms,
        )
        decision = _to_text(raw[0]) or ""
        result = StartWorkflowResult(
            decision=decision,  # type: ignore[arg-type]
            run_id=_to_text(raw[1]) or None,
            status=_to_text(raw[2]) or "",
            run_no=_to_int(raw[3]),
            result_payload=_to_bytes(raw[4]),
            error_payload=_to_bytes(raw[5]),
        )
        return result

    async def get_workflow_result(self, workflow_id: str) -> WorkflowResultSnapshot | None:
        mapping = await self.redis.hgetall(
            workflow_control(self.settings.key_prefix, workflow_id)
        )
        if not mapping:
            return None
        record = _decode_hash(mapping)
        return WorkflowResultSnapshot(
            workflow_id=workflow_id,
            run_id=_to_text(record.get("current_run_id")),
            status=_to_text(record.get("status")) or "",
            run_no=_to_int(record.get("run_no")),
            result_payload=_to_bytes(record.get("result_payload")),
            error_payload=_to_bytes(record.get("error_payload")),
        )

    async def wait_for_workflow_result(
        self,
        workflow_id: str,
        *,
        timeout_ms: int,
    ) -> WorkflowResultSnapshot | None:
        snapshot = await self.get_workflow_result(workflow_id)
        if snapshot is None:
            return None
        if snapshot.status in {"completed", "failed"} or timeout_ms <= 0:
            return snapshot

        channel = workflow_result_channel(self.settings.key_prefix, workflow_id)
        pubsub = self.redis.pubsub()
        try:
            await pubsub.subscribe(channel)
            snapshot = await self.get_workflow_result(workflow_id)
            if snapshot is None:
                return None
            if snapshot.status in {"completed", "failed"}:
                return snapshot
            deadline = time.monotonic() + (timeout_ms / 1000)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(remaining, 0.25),
                )
                if message is not None:
                    snapshot = await self.get_workflow_result(workflow_id)
                    if snapshot is not None:
                        return snapshot
            return await self.get_workflow_result(workflow_id)
        finally:
            try:
                await pubsub.unsubscribe(channel)
            finally:
                await pubsub.aclose()

    async def complete_workflow_task(
        self,
        *,
        run_id: str,
        workflow_task_id: str,
        attempt_no: int,
        command: WorkflowTaskCommand,
    ) -> WorkflowTaskCompletionResult:
        workflow_id = await self._workflow_id_for_run(run_id)
        if workflow_id is None:
            return WorkflowTaskCompletionResult(outcome="missing")

        if command.kind == "schedule_activity":
            if command.activity_name is None or command.activity_task_queue is None:
                raise ValueError("schedule_activity requires activity name and task queue.")
            await self.ensure_activity_queue(command.activity_task_queue)

        run_state_key = workflow_state(self.settings.key_prefix, run_id)
        history_key = workflow_history(self.settings.key_prefix, run_id)
        timer_member = f"workflow-timeout:{run_id}:{workflow_task_id}:{attempt_no}"

        if command.kind == "schedule_activity":
            retry_policy = command.retry_policy or RetryPolicyConfig(max_attempts=1)
            raw = await self.redis.eval(
                APPLY_WORKFLOW_TASK_COMPLETION,
                7,
                workflow_control(self.settings.key_prefix, workflow_id),
                run_state_key,
                history_key,
                f"{self.settings.key_prefix}:activity:",
                activity_queue(self.settings.key_prefix, command.activity_task_queue),
                timers(self.settings.key_prefix),
                workflow_result_channel(self.settings.key_prefix, workflow_id),
                _now_ms(),
                workflow_task_id,
                attempt_no,
                self.settings.workflow_task_timeout_ms,
                command.kind,
                timer_member,
                command.activity_name,
                command.activity_task_queue,
                command.input_payload or b"",
                command.schedule_to_close_timeout_ms or 0,
                retry_policy.max_attempts or 1,
                retry_policy.initial_interval_ms or 0,
                retry_policy.backoff_coefficient or 0,
                retry_policy.max_interval_ms or 0,
            )
        else:
            terminal_payload = command.result_payload if command.kind == "complete_workflow" else command.error_payload
            raw = await self.redis.eval(
                APPLY_WORKFLOW_TASK_COMPLETION,
                7,
                workflow_control(self.settings.key_prefix, workflow_id),
                run_state_key,
                history_key,
                activity_state(self.settings.key_prefix, "__unused__"),
                activity_queue(self.settings.key_prefix, "__unused__"),
                timers(self.settings.key_prefix),
                workflow_result_channel(self.settings.key_prefix, workflow_id),
                _now_ms(),
                workflow_task_id,
                attempt_no,
                self.settings.workflow_task_timeout_ms,
                command.kind,
                timer_member,
                terminal_payload or b"",
            )

        return WorkflowTaskCompletionResult(
            outcome=_to_text(raw[0]) or "",
            run_id=_to_text(raw[1]) or None,
            activity_execution_id=_to_text(raw[2]) or None,
            terminal_status="completed" if command.kind == "complete_workflow" else (
                "failed" if command.kind == "fail_workflow" else None
            ),
        )

    async def complete_activity_task(
        self,
        *,
        activity_execution_id: str,
        attempt_no: int,
        status: str,
        payload: bytes | None,
    ) -> ActivityTaskCompletionResult:
        activity_key = activity_state(self.settings.key_prefix, activity_execution_id)
        mapping = await self.redis.hgetall(activity_key)
        if not mapping:
            return ActivityTaskCompletionResult(outcome="missing")

        record = _decode_hash(mapping)
        run_id = _to_text(record.get("run_id"))
        if run_id is None:
            return ActivityTaskCompletionResult(outcome="missing")

        workflow_task_queue = await self._workflow_task_queue_for_run(run_id)
        workflow_id = await self._workflow_id_for_run(run_id)
        if workflow_id is None:
            return ActivityTaskCompletionResult(outcome="missing")

        raw = await self.redis.eval(
            APPLY_ACTIVITY_COMPLETION,
            6,
            workflow_control(self.settings.key_prefix, workflow_id),
            workflow_state(self.settings.key_prefix, run_id),
            workflow_history(self.settings.key_prefix, run_id),
            activity_key,
            workflow_queue(self.settings.key_prefix, workflow_task_queue),
            timers(self.settings.key_prefix),
            _now_ms(),
            status,
            attempt_no,
            payload or b"",
            self.settings.workflow_task_timeout_ms,
            f"activity-timeout:{activity_execution_id}:{attempt_no}",
        )
        return ActivityTaskCompletionResult(
            outcome=_to_text(raw[0]) or "",
            run_id=_to_text(raw[1]) or None,
            next_workflow_task_id=_to_text(raw[2]) or None,
            activity_execution_id=activity_execution_id,
        )

    async def pop_due_timer(self) -> str | None:
        due = await self.redis.zrangebyscore(
            timers(self.settings.key_prefix),
            min=0,
            max=_now_ms(),
            start=0,
            num=1,
        )
        if not due:
            return None
        member = _to_text(due[0]) or ""
        removed = await self.redis.zrem(timers(self.settings.key_prefix), due[0])
        if removed != 1:
            return None
        return member

    async def apply_timer(self, member: str) -> dict[str, Any]:
        now_ms = _now_ms()
        timer_kind, logical_id, attempt_no, run_id, activity_task_queue = await self._parse_timer_member(member)

        timeout_error_payload = b""
        run_state_key = workflow_state(self.settings.key_prefix, run_id) if run_id else workflow_state(self.settings.key_prefix, "__unused__")
        history_key = workflow_history(self.settings.key_prefix, run_id) if run_id else workflow_history(self.settings.key_prefix, "__unused__")
        activity_key = activity_state(self.settings.key_prefix, logical_id) if timer_kind.startswith("activity") else activity_state(self.settings.key_prefix, "__unused__")
        workflow_queue_key = workflow_queue(
            self.settings.key_prefix,
            await self._workflow_task_queue_for_run(run_id) if run_id else "__unused__",
        )
        activity_queue_key = activity_queue(
            self.settings.key_prefix,
            activity_task_queue or "__unused__",
        )

        if timer_kind == "activity-timeout":
            timeout_error_payload = packb(
                {
                    "type": "activity_timeout",
                    "activity_execution_id": logical_id,
                    "attempt_no": attempt_no,
                }
            )

        raw = await self.redis.eval(
            APPLY_TIMER,
            6,
            run_state_key,
            history_key,
            activity_key,
            workflow_queue_key,
            activity_queue_key,
            timers(self.settings.key_prefix),
            now_ms,
            timer_kind,
            logical_id,
            attempt_no,
            self.settings.workflow_task_timeout_ms,
            timeout_error_payload,
        )
        return {
            "outcome": _to_text(raw[0]) or "",
            "run_id": _to_text(raw[1]) or None,
            "logical_id": _to_text(raw[2]) or None,
            "timer_kind": timer_kind,
        }

    async def get_history(self, run_id: str) -> list[Any]:
        entries = await self.redis.lrange(
            workflow_history(self.settings.key_prefix, run_id),
            0,
            -1,
        )
        return [unpackb(entry) for entry in entries]

    async def get_run_state(self, run_id: str) -> dict[str, Any]:
        return _decode_hash(
            await self.redis.hgetall(workflow_state(self.settings.key_prefix, run_id))
        )

    async def get_activity_state(self, activity_execution_id: str) -> dict[str, Any]:
        return _decode_hash(
            await self.redis.hgetall(
                activity_state(self.settings.key_prefix, activity_execution_id)
            )
        )

    async def cleanup_stale_pending_entries(self) -> int:
        cleaned = 0
        cleaned += await self._cleanup_pending_kind("workflow")
        cleaned += await self._cleanup_pending_kind("activity")
        return cleaned

    async def _cleanup_pending_kind(self, kind: str) -> int:
        registry_key = (
            workflow_task_queues(self.settings.key_prefix)
            if kind == "workflow"
            else activity_task_queues(self.settings.key_prefix)
        )
        group_name = (
            self.settings.workflow_consumer_group
            if kind == "workflow"
            else self.settings.activity_consumer_group
        )
        stream_builder = workflow_queue if kind == "workflow" else activity_queue
        task_queues = await self.redis.smembers(registry_key)
        cleaned = 0

        for queue_name_raw in task_queues:
            queue_name = _to_text(queue_name_raw)
            if not queue_name:
                continue
            stream_key = stream_builder(self.settings.key_prefix, queue_name)
            try:
                claimed = await self.redis.xautoclaim(
                    stream_key,
                    group_name,
                    "fluxi-scheduler",
                    self.settings.pending_idle_threshold_ms,
                    start_id="0-0",
                    count=self.settings.pending_claim_count,
                )
            except ResponseError as exc:
                if "NOGROUP" in str(exc).upper():
                    await self._ensure_stream_group(
                        stream_key=stream_key,
                        group_name=group_name,
                        registry_key=registry_key,
                        task_queue=queue_name,
                    )
                    continue
                raise
            messages = claimed[1] if isinstance(claimed, (list, tuple)) and len(claimed) > 1 else []
            for message_id, values in messages:
                payload = values.get(b"payload") or values.get("payload")
                if payload is None:
                    continue
                decoded = unpackb(payload)
                if kind == "workflow":
                    is_current = await self._is_current_workflow_task(
                        decoded["run_id"],
                        decoded["workflow_task_id"],
                        int(decoded["attempt_no"]),
                    )
                else:
                    is_current = await self._is_current_activity_task(
                        decoded["activity_execution_id"],
                        int(decoded["attempt_no"]),
                    )
                if not is_current:
                    cleaned += await self.redis.xack(stream_key, group_name, message_id)
        return cleaned

    async def _is_current_workflow_task(
        self,
        run_id: str,
        workflow_task_id: str,
        attempt_no: int,
    ) -> bool:
        mapping = await self.redis.hmget(
            workflow_state(self.settings.key_prefix, run_id),
            "open_workflow_task_id",
            "open_workflow_task_attempt_no",
            "status",
        )
        return (
            (_to_text(mapping[0]) or "") == workflow_task_id
            and (_to_int(mapping[1]) or 0) == attempt_no
            and (_to_text(mapping[2]) or "") == "running"
        )

    async def _is_current_activity_task(
        self,
        activity_execution_id: str,
        attempt_no: int,
    ) -> bool:
        mapping = await self.redis.hmget(
            activity_state(self.settings.key_prefix, activity_execution_id),
            "current_attempt_no",
            "status",
        )
        return (
            (_to_int(mapping[0]) or 0) == attempt_no
            and (_to_text(mapping[1]) or "") in {"scheduled", "running", "retry_pending"}
        )

    async def _parse_timer_member(
        self,
        member: str,
    ) -> tuple[str, str, int, str | None, str | None]:
        if member.startswith("workflow-timeout:"):
            body = member.removeprefix("workflow-timeout:")
            run_id, workflow_task_id, attempt = body.rsplit(":", 2)
            return "workflow-timeout", workflow_task_id, int(attempt), run_id, None
        if member.startswith("activity-timeout:"):
            body = member.removeprefix("activity-timeout:")
            activity_execution_id, attempt = body.rsplit(":", 1)
            activity_record = await self.get_activity_state(activity_execution_id)
            return (
                "activity-timeout",
                activity_execution_id,
                int(attempt),
                _to_text(activity_record.get("run_id")),
                _to_text(activity_record.get("task_queue")),
            )
        if member.startswith("activity-retry:"):
            body = member.removeprefix("activity-retry:")
            activity_execution_id, attempt = body.rsplit(":", 1)
            activity_record = await self.get_activity_state(activity_execution_id)
            return (
                "activity-retry",
                activity_execution_id,
                int(attempt),
                _to_text(activity_record.get("run_id")),
                _to_text(activity_record.get("task_queue")),
            )
        raise ValueError(f"Unknown timer member: {member}")

    async def _workflow_task_queue_for_run(self, run_id: str | None) -> str:
        if run_id is None:
            return "__unused__"
        queue = await self.redis.hget(
            workflow_state(self.settings.key_prefix, run_id),
            "workflow_task_queue",
        )
        return _to_text(queue) or "__unused__"

    async def _workflow_id_for_run(self, run_id: str | None) -> str | None:
        if run_id is None:
            return None
        workflow_id = await self.redis.hget(
            workflow_state(self.settings.key_prefix, run_id),
            "workflow_id",
        )
        return _to_text(workflow_id)
