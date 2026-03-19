import asyncio
import hashlib
import json
import os
import socket
import time
import uuid
from dataclasses import dataclass

import redis
from redis.asyncio import Redis

from logging_utils import log_event
from models import ParticipantResult


@dataclass(frozen=True)
class PendingCommandResult:
    status: str
    ok: bool
    retryable: bool
    detail: str | None


class SagaCommandBus:
    SCHEMA_VERSION = "v1"

    COMMAND_STREAM_PREFIX = "saga:cmd:"
    RESULT_STREAM_PREFIX = "saga:res:"
    PENDING_PREFIX = "saga:mq:pending:"
    CURSOR_PREFIX = "saga:mq:cursor:"
    LEASE_PREFIX = "saga:mq:lease:p"
    METRIC_PREFIX = "saga:mq:metric:"

    def __init__(
        self,
        db: Redis,
        logger,
        stream_partitions: int = 4,
        response_timeout_ms: int = 3000,
        command_stream_maxlen: int = 100000,
        result_stream_maxlen: int = 100000,
        pending_ttl_seconds: int = 3600,
        poll_interval_seconds: float = 0.05,
        enable_dispatcher: bool = True,
        dispatcher_block_ms: int = 1000,
        dispatch_lease_ttl_seconds: int = 10,
        dispatch_renew_interval_seconds: float = 2.0,
        owner_id: str | None = None,
        participants: tuple[str, ...] = ("stock", "payment"),
    ):
        self.db = db
        self.logger = logger
        self.stream_partitions = int(stream_partitions)
        self.response_timeout_ms = int(response_timeout_ms)
        self.command_stream_maxlen = int(command_stream_maxlen)
        self.result_stream_maxlen = int(result_stream_maxlen)
        self.pending_ttl_seconds = int(pending_ttl_seconds)
        self.poll_interval_seconds = float(poll_interval_seconds)
        self.enable_dispatcher = bool(enable_dispatcher)
        self.dispatcher_block_ms = int(dispatcher_block_ms)
        self.dispatch_lease_ttl_seconds = int(dispatch_lease_ttl_seconds)
        self.dispatch_renew_interval_seconds = float(dispatch_renew_interval_seconds)
        self.participants = participants
        self.owner_id = owner_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._dispatcher_task: asyncio.Task | None = None
        self._owned_partitions: set[int] = set()
        self._stream_cursors: dict[str, str] = {}

    async def start(self):
        if self._dispatcher_task is not None and not self._dispatcher_task.done():
            return
        await self._ensure_cursor_keys()
        if not self.enable_dispatcher:
            self._log(
                "saga_mq_dispatcher_disabled",
                owner_id=self.owner_id,
                participants=list(self.participants),
                stream_partitions=self.stream_partitions,
            )
            return
        self._dispatcher_task = asyncio.create_task(self._dispatcher_loop())
        self._log(
            "saga_mq_dispatcher_started",
            owner_id=self.owner_id,
            participants=list(self.participants),
            stream_partitions=self.stream_partitions,
        )

    async def stop(self):
        if self._dispatcher_task is None:
            return
        self._dispatcher_task.cancel()
        try:
            await self._dispatcher_task
        except asyncio.CancelledError:
            pass
        self._dispatcher_task = None
        self._owned_partitions.clear()
        self._log("saga_mq_dispatcher_stopped")

    async def recover_stale_pending(self, stale_after_ms: int):
        now_ms = int(time.time() * 1000)
        stale_after_ms = int(stale_after_ms)
        cursor = 0
        stale_count = 0

        while True:
            cursor, keys = await self.db.scan(cursor=cursor, match=f"{self.PENDING_PREFIX}*", count=200)
            for pending_key in keys:
                key_str = self._decode(pending_key)
                data = await self.db.hgetall(key_str)
                decoded = self._decode_dict(data)
                status = decoded.get("status", "")
                if status != "pending":
                    continue

                created_at_ms = int(decoded.get("created_at_ms", "0") or "0")
                if created_at_ms <= 0:
                    continue
                if now_ms - created_at_ms < stale_after_ms:
                    continue

                await self.db.hset(
                    key_str,
                    mapping={
                        "status": "timed_out",
                        "ok": "0",
                        "retryable": "1",
                        "detail": "Timed out while coordinator was unavailable",
                        "completed_at_ms": str(now_ms),
                    },
                )
                stale_count += 1
                await self._incr_metric("stale_pending_recovered_total")

            if cursor == 0:
                break

        if stale_count > 0:
            self._log("saga_mq_recovered_stale_pending", stale_count=stale_count, stale_after_ms=stale_after_ms)

    async def request(
        self,
        participant: str,
        action: str,
        tx_id: str,
        payload: dict,
        attempt: int,
    ) -> ParticipantResult:
        if participant not in self.participants:
            return ParticipantResult(ok=False, retryable=False, detail=f"Unknown participant: {participant}")

        partition = self._partition_for_tx(tx_id)
        correlation_id = str(uuid.uuid4())
        msg_id = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        pending_key = self._pending_key(correlation_id)
        payload_json = json.dumps(payload, separators=(",", ":"), default=str)

        await self.db.hset(
            pending_key,
            mapping={
                "status": "pending",
                "correlation_id": correlation_id,
                "msg_id": msg_id,
                "tx_id": tx_id,
                "participant": participant,
                "action": action,
                "partition": str(partition),
                "attempt": str(int(attempt)),
                "payload": payload_json,
                "schema_version": self.SCHEMA_VERSION,
                "created_at_ms": str(now_ms),
            },
        )
        await self.db.expire(pending_key, self.pending_ttl_seconds)

        command_stream = self._command_stream(participant, partition)
        try:
            await self.db.xadd(
                name=command_stream,
                fields={
                    "schema_version": self.SCHEMA_VERSION,
                    "msg_id": msg_id,
                    "correlation_id": correlation_id,
                    "tx_id": tx_id,
                    "participant": participant,
                    "action": action,
                    "partition": str(partition),
                    "attempt": str(int(attempt)),
                    "sent_at_ms": str(now_ms),
                    "payload": payload_json,
                },
                maxlen=self.command_stream_maxlen,
                approximate=True,
            )
        except redis.exceptions.RedisError as exc:
            await self.db.hset(
                pending_key,
                mapping={
                    "status": "failed",
                    "ok": "0",
                    "retryable": "1",
                    "detail": f"Saga MQ publish failed: {exc}",
                    "completed_at_ms": str(int(time.time() * 1000)),
                },
            )
            await self._incr_metric("publish_failed_total")
            self._log(
                "saga_mq_publish_failed",
                level="warning",
                participant=participant,
                action=action,
                tx_id=tx_id,
                correlation_id=correlation_id,
                attempt=attempt,
                detail=str(exc),
            )
            return ParticipantResult(ok=False, retryable=True, detail="Saga MQ publish failed")
        await self._incr_metric("published_total")

        self._log(
            "saga_mq_published",
            participant=participant,
            action=action,
            tx_id=tx_id,
            correlation_id=correlation_id,
            attempt=attempt,
            partition=partition,
            command_stream=command_stream,
        )

        result = await self._await_result(correlation_id, self.response_timeout_ms)
        return ParticipantResult(
            ok=result.ok,
            retryable=result.retryable,
            detail=result.detail,
            correlation_id=correlation_id,
            status=result.status,
        )

    async def await_late_result(self, correlation_id: str, timeout_ms: int) -> ParticipantResult:
        pending_key = self._pending_key(correlation_id)
        deadline = asyncio.get_running_loop().time() + (int(timeout_ms) / 1000.0)

        while asyncio.get_running_loop().time() < deadline:
            raw = await self.db.hgetall(pending_key)
            decoded = self._decode_dict(raw)
            status = decoded.get("status", "")
            if status in ("completed", "failed"):
                ok = decoded.get("ok", "0").lower() in ("1", "true", "yes")
                retryable = decoded.get("retryable", "0").lower() in ("1", "true", "yes")
                detail = decoded.get("detail") or None
                return ParticipantResult(
                    ok=ok,
                    retryable=retryable,
                    detail=detail,
                    correlation_id=correlation_id,
                    status=status,
                )
            await asyncio.sleep(self.poll_interval_seconds)

        raw = await self.db.hgetall(pending_key)
        decoded = self._decode_dict(raw)
        status = decoded.get("status", "") or "missing"
        ok = decoded.get("ok", "0").lower() in ("1", "true", "yes")
        retryable = decoded.get("retryable", "0").lower() in ("1", "true", "yes")
        detail = decoded.get("detail") or None
        if status == "missing" and detail is None:
            detail = "Saga MQ pending result missing"
        return ParticipantResult(
            ok=ok,
            retryable=retryable,
            detail=detail,
            correlation_id=correlation_id,
            status=status,
        )

    async def _await_result(self, correlation_id: str, timeout_ms: int) -> PendingCommandResult:
        pending_key = self._pending_key(correlation_id)
        deadline = asyncio.get_running_loop().time() + (int(timeout_ms) / 1000.0)

        while asyncio.get_running_loop().time() < deadline:
            raw = await self.db.hgetall(pending_key)
            decoded = self._decode_dict(raw)
            status = decoded.get("status", "")
            if status in ("completed", "failed", "timed_out"):
                ok = decoded.get("ok", "0").lower() in ("1", "true", "yes")
                retryable = decoded.get("retryable", "0").lower() in ("1", "true", "yes")
                detail = decoded.get("detail") or None
                return PendingCommandResult(status=status, ok=ok, retryable=retryable, detail=detail)
            await asyncio.sleep(self.poll_interval_seconds)

        await self.db.hset(
            pending_key,
            mapping={
                "status": "timed_out",
                "ok": "0",
                "retryable": "1",
                "detail": "Saga MQ response timeout",
                "completed_at_ms": str(int(time.time() * 1000)),
            },
        )
        await self._incr_metric("response_timeout_total")
        return PendingCommandResult(
            status="timed_out",
            ok=False,
            retryable=True,
            detail="Saga MQ response timeout",
        )

    async def _dispatcher_loop(self):
        while True:
            try:
                owned_partitions = await self._refresh_partition_leases()
                if owned_partitions != self._owned_partitions:
                    self._owned_partitions = owned_partitions
                    self._log(
                        "saga_mq_partition_ownership_changed",
                        owner_id=self.owner_id,
                        owned_partitions=sorted(self._owned_partitions),
                    )

                if not owned_partitions:
                    await asyncio.sleep(self.dispatch_renew_interval_seconds)
                    continue

                streams = await self._load_result_cursors(owned_partitions)
                block_ms = min(
                    self.dispatcher_block_ms,
                    max(100, int(self.dispatch_renew_interval_seconds * 1000)),
                )
                entries = await self.db.xread(streams=streams, count=128, block=block_ms)
            except redis.exceptions.RedisError as exc:
                await self._incr_metric("dispatcher_read_failed_total")
                self._log("saga_mq_dispatcher_read_failed", level="warning", detail=str(exc))
                await asyncio.sleep(0.2)
                continue

            if not entries:
                continue

            for stream_name_raw, stream_entries in entries:
                stream_name = self._decode(stream_name_raw)
                participant, partition = self._parse_result_stream(stream_name)
                if participant is None:
                    continue
                if partition not in owned_partitions:
                    continue

                for message_id_raw, fields_raw in stream_entries:
                    message_id = self._decode(message_id_raw)
                    fields = self._decode_dict(fields_raw)
                    correlation_id = fields.get("correlation_id", "")
                    if not correlation_id:
                        await self._incr_metric("result_missing_correlation_total")
                        self._log(
                            "saga_mq_result_missing_correlation",
                            level="warning",
                            stream=stream_name,
                            message_id=message_id,
                        )
                        streams[stream_name] = message_id
                        self._stream_cursors[stream_name] = message_id
                        await self.db.set(self._cursor_key(participant, partition), message_id)
                        continue

                    await self._apply_result(correlation_id, fields)

                    streams[stream_name] = message_id
                    self._stream_cursors[stream_name] = message_id
                    await self.db.set(self._cursor_key(participant, partition), message_id)

    async def _apply_result(self, correlation_id: str, fields: dict[str, str]):
        pending_key = self._pending_key(correlation_id)
        data = await self.db.hgetall(pending_key)
        decoded = self._decode_dict(data)
        if not decoded:
            await self._incr_metric("result_without_pending_total")
            self._log(
                "saga_mq_result_without_pending",
                level="warning",
                correlation_id=correlation_id,
                tx_id=fields.get("tx_id"),
                participant=fields.get("participant"),
                action=fields.get("action"),
            )
            return

        if decoded.get("status") not in {"pending", "timed_out"}:
            await self._incr_metric("result_for_non_pending_total")
            return

        ok = fields.get("ok", "0")
        retryable = fields.get("retryable", "0")
        detail = fields.get("detail", "")
        await self.db.hset(
            pending_key,
            mapping={
                "status": "completed",
                "ok": "1" if ok.lower() in ("1", "true", "yes") else "0",
                "retryable": "1" if retryable.lower() in ("1", "true", "yes") else "0",
                "detail": detail,
                "completed_at_ms": str(int(time.time() * 1000)),
            },
        )
        await self.db.expire(pending_key, self.pending_ttl_seconds)
        await self._incr_metric("result_applied_total")
        if ok.lower() in ("1", "true", "yes"):
            await self._incr_metric("result_ok_total")
        else:
            await self._incr_metric("result_fail_total")
        self._log(
            "saga_mq_result_applied",
            tx_id=fields.get("tx_id"),
            participant=fields.get("participant"),
            action=fields.get("action"),
            correlation_id=correlation_id,
            ok=ok,
            retryable=retryable,
            detail=detail,
        )

    async def _ensure_cursor_keys(self):
        for participant in self.participants:
            for partition in range(self.stream_partitions):
                await self.db.setnx(self._cursor_key(participant, partition), "0-0")

    async def _load_result_cursors(self, partitions: set[int]) -> dict[str, str]:
        streams: dict[str, str] = {}
        for participant in self.participants:
            for partition in sorted(partitions):
                stream = self._result_stream(participant, partition)
                cached = self._stream_cursors.get(stream)
                if cached is not None:
                    streams[stream] = cached
                    continue
                cursor = await self.db.get(self._cursor_key(participant, partition))
                value = self._decode(cursor) if cursor is not None else "0-0"
                streams[stream] = value
                self._stream_cursors[stream] = value
        return streams

    async def _refresh_partition_leases(self) -> set[int]:
        owned: set[int] = set()
        for partition in range(self.stream_partitions):
            lease_key = self._lease_key(partition)
            value = await self.db.get(lease_key)
            current_owner = self._decode(value)
            if current_owner == self.owner_id:
                await self.db.expire(lease_key, self.dispatch_lease_ttl_seconds)
                owned.add(partition)
                continue
            if value is None:
                acquired = await self.db.set(
                    lease_key,
                    self.owner_id,
                    nx=True,
                    ex=self.dispatch_lease_ttl_seconds,
                )
                if acquired:
                    owned.add(partition)
        return owned

    def _partition_for_tx(self, tx_id: str) -> int:
        digest = hashlib.sha256(tx_id.encode("utf-8")).hexdigest()
        return int(digest, 16) % self.stream_partitions

    def _command_stream(self, participant: str, partition: int) -> str:
        return f"{self.COMMAND_STREAM_PREFIX}{participant}:p{partition}"

    def _result_stream(self, participant: str, partition: int) -> str:
        return f"{self.RESULT_STREAM_PREFIX}{participant}:p{partition}"

    def _pending_key(self, correlation_id: str) -> str:
        return f"{self.PENDING_PREFIX}{correlation_id}"

    def _cursor_key(self, participant: str, partition: int) -> str:
        return f"{self.CURSOR_PREFIX}{participant}:p{partition}"

    def _lease_key(self, partition: int) -> str:
        return f"{self.LEASE_PREFIX}{partition}"

    def _metric_key(self, metric_name: str) -> str:
        return f"{self.METRIC_PREFIX}{metric_name}"

    @staticmethod
    def _decode(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode()
        return str(value)

    def _decode_dict(self, raw: dict) -> dict[str, str]:
        return {self._decode(key): self._decode(value) for key, value in raw.items()}

    def _parse_result_stream(self, stream_name: str) -> tuple[str | None, int]:
        # Expected shape: saga:res:<participant>:p<partition>
        parts = stream_name.split(":")
        if len(parts) != 4 or parts[0] != "saga" or parts[1] != "res":
            self._log("saga_mq_unknown_result_stream", level="warning", stream=stream_name)
            return None, -1
        participant = parts[2]
        partition_part = parts[3]
        if not partition_part.startswith("p"):
            self._log("saga_mq_invalid_result_partition", level="warning", stream=stream_name)
            return None, -1
        try:
            return participant, int(partition_part[1:])
        except ValueError:
            self._log("saga_mq_invalid_result_partition", level="warning", stream=stream_name)
            return None, -1

    async def _incr_metric(self, metric_name: str, delta: int = 1):
        try:
            await self.db.incrby(self._metric_key(metric_name), int(delta))
        except redis.exceptions.RedisError:
            pass

    async def get_metrics_snapshot(self) -> dict:
        metric_keys = [
            "published_total",
            "publish_failed_total",
            "response_timeout_total",
            "stale_pending_recovered_total",
            "dispatcher_read_failed_total",
            "result_missing_correlation_total",
            "result_without_pending_total",
            "result_for_non_pending_total",
            "result_applied_total",
            "result_ok_total",
            "result_fail_total",
        ]
        counters: dict[str, int] = {}
        for name in metric_keys:
            raw = await self.db.get(self._metric_key(name))
            counters[name] = int(self._decode(raw) or "0")

        command_stream_lengths: dict[str, int] = {}
        result_stream_lengths: dict[str, int] = {}
        for participant in self.participants:
            for partition in range(self.stream_partitions):
                cmd_stream = self._command_stream(participant, partition)
                res_stream = self._result_stream(participant, partition)
                try:
                    command_stream_lengths[cmd_stream] = int(await self.db.xlen(cmd_stream))
                except redis.exceptions.RedisError:
                    command_stream_lengths[cmd_stream] = -1
                try:
                    result_stream_lengths[res_stream] = int(await self.db.xlen(res_stream))
                except redis.exceptions.RedisError:
                    result_stream_lengths[res_stream] = -1

        pending_counts = await self._count_pending_statuses()
        return {
            "owner_id": self.owner_id,
            "dispatcher_enabled": self.enable_dispatcher,
            "owned_partitions": sorted(self._owned_partitions),
            "stream_partitions": self.stream_partitions,
            "counters": counters,
            "pending": pending_counts,
            "command_stream_lengths": command_stream_lengths,
            "result_stream_lengths": result_stream_lengths,
        }

    async def _count_pending_statuses(self) -> dict[str, int]:
        cursor = 0
        counts: dict[str, int] = {
            "pending": 0,
            "completed": 0,
            "failed": 0,
            "timed_out": 0,
            "unknown": 0,
            "total": 0,
        }
        while True:
            cursor, keys = await self.db.scan(cursor=cursor, match=f"{self.PENDING_PREFIX}*", count=200)
            for pending_key in keys:
                key_str = self._decode(pending_key)
                raw = await self.db.hget(key_str, "status")
                status = self._decode(raw) or "unknown"
                counts["total"] += 1
                if status in counts:
                    counts[status] += 1
                else:
                    counts["unknown"] += 1
            if cursor == 0:
                break
        return counts

    def _log(self, event: str, level: str = "info", **fields):
        log_event(
            self.logger,
            event=event,
            level=level,
            service="order-service",
            component="saga-mq",
            **fields,
        )
